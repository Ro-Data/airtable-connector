import json
import os
import re

import numpy as np
import pandas as pd
import sqlalchemy
import structlog

from .lib.airtable import AirtableClient, AirtableIteratorError
from .lib.snowflake import get_engine

log = structlog.get_logger()


def parse_table_name(name):
    parts = name.split(".")
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2:
        database = None
        schema, table = parts
    else:
        raise ValueError(f"Invalid table name: {name}")
    return {"database": database, "schema": schema, "table": table}


def process_record(record):
    fields = record.pop("fields", {})
    overlap = set(record.keys()) & set(fields.keys())
    if overlap:
        log.warn("names overlap between record and fields", overlap=overlap)
    record.update(fields)
    return record


def process_records(records):
    return [process_record(record) for record in records]


def rename_column(name):
    result = re.sub(r"[^a-z0-9_]+", "_", name.lower())
    result = re.sub(r"_{2,}", "_", result)
    return result


def maybe_jsonify(value):
    if isinstance(value, (list, dict)):
        is_null = False
    else:
        is_null = pd.isna(value)
    if is_null:
        return value
    return json.dumps(value)


def result_to_df(data, field_info):
    """Return `data` from Airtable converted to a DataFrame.

    The records returned from Airtable won't include any fields for which all values
    are NULL, but we always want those fields to exist in the database, so we create
    the dataframe with the full list of fields returned by Airtable's metadata API
    (plus the implicit `id` and `createdTime` fields).
    """
    columns = ["id"] + list(field_info.keys()) + ["createdTime"]
    df = pd.DataFrame(process_records(data), columns=columns)
    for field_name, field_type in field_info.items():
        # Airtable returns some field types as objects or arrays - serialize them as
        # JSON strings
        if field_type in ("multipleSelects", "multilineText", "formula"):
            df[field_name] = df[field_name].apply(maybe_jsonify)
    df.rename(columns=rename_column, inplace=True)
    return df.convert_dtypes()


def table_exists(connection, schema, table):
    try:
        connection.execute(f"describe table identifier('{schema}.{table}')")
    except sqlalchemy.exc.ProgrammingError:
        return False
    return True


def materialize(
    connection,
    df,
    schema,
    table,
    chunksize=4096,
    method="multi",
    if_exists="fail",
    float_db_dtype=sqlalchemy.types.Numeric(38, 6),
):
    if float_db_dtype:
        dtype = {
            col: float_db_dtype
            for (col, dtype) in df.dtypes.to_dict().items()
            if dtype == np.float64
        }
    else:
        dtype = {}
    connection.execute(f"create schema if not exists {schema}")
    df.to_sql(
        table,
        connection,
        schema=schema,
        index=False,
        chunksize=chunksize,
        method=method,
        if_exists=if_exists,
        dtype=dtype,
    )


def _load_chunks(connection, chunks, field_info, destination):
    database, schema, table = parse_table_name(destination).values()
    temporary_table = f"{table}__loader_tmp"
    count = 0
    if database:
        schema = f"{database}.{schema}"
    # Delete the temporary table target if an old one is hanging around
    connection.execute(f"drop table if exists identifier('{schema}.{temporary_table}')")
    # Load the data into its temporary location
    for chunk in chunks:
        df = result_to_df(chunk, field_info)
        materialize(
            connection,
            df,
            schema,
            temporary_table,
            if_exists="append",
        )
        count += len(df)
    # Make sure the table is created in the db even if it's empty. This is ugly,
    # including in that every column will have type `string`, but the types will be
    # fixed up as soon as there's actual data to load
    if count == 0:
        nulls_df = result_to_df([], field_info)
        nulls_df = pd.concat(
            [nulls_df, pd.DataFrame([{c: None for c in nulls_df.columns}])]
        )
        materialize(
            connection,
            nulls_df,
            schema,
            temporary_table,
        )
        connection.execute(f"delete from identifier('{schema}.{temporary_table}') ")
    # Move the temporary table into place with its final name
    if table_exists(connection, schema, table):
        connection.execute(
            f"alter table identifier('{schema}.{temporary_table}') "
            f"swap with identifier('{schema}.{table}')"
        )
        connection.execute(f"drop table identifier('{schema}.{temporary_table}') ")
    else:
        connection.execute(
            f"alter table identifier('{schema}.{temporary_table}') "
            f"rename to identifier('{schema}.{table}')"
        )
    return count


def rechunked(chunks, n):
    xs = []
    for chunk in chunks:
        xs.extend(chunk)
        if len(xs) >= n:
            yield xs
            xs = []
    if xs:
        yield xs


def load(connection, airtable, table_name, field_info, destination):
    restarts = 0
    while True:
        try:
            chunks = airtable.iter_chunks(table_name)
            # Airtable will give us the records in chunks of 100, which is
            # inefficiently small for loading into Snowflake, so buffer them up into
            # chunks of a few thousand
            chunks = rechunked(chunks, 5000)
            return _load_chunks(connection, chunks, field_info, destination)
        except AirtableIteratorError:
            if restarts == 3:
                raise
            else:
                restarts += 1
                log.warn(
                    "airtable list records iterator not available, restarting",
                    restart=restarts,
                )


def run(base_id, table_name, destination):
    engine = get_engine()
    airtable = AirtableClient(base_id, os.environ["AIRTABLE_API_KEY"])
    log.info("reading airtable metadata")
    metadata = airtable.get_metadata()
    log.info("loading data", table_name=table_name, destination=destination)
    count = load(engine, airtable, table_name, metadata[table_name], destination)
    log.info("done loading data", count=count, destination=destination)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-id", required=True)
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--destination", required=True)

    args = parser.parse_args()

    run(args.base_id, args.table_name, args.destination)
