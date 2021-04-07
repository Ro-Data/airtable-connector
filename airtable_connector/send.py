import collections
import concurrent.futures
import datetime
import decimal
import os

import structlog
import yaml

from .lib.airtable import AirtableClient
from .lib.snowflake import get_engine

log = structlog.get_logger()


def read_yaml(path):
    with open(path) as infile:
        return yaml.safe_load(infile)


def get_field_mappings(airtable_fields):
    result = collections.defaultdict(list)
    result["id"].append("id")
    for field in airtable_fields:
        result[field].append(field.lower().replace(" ", "_"))
    return result


def read_config(config_file):
    config = read_yaml(config_file)
    config["tables"] = [
        item if isinstance(item, dict) else {"table": item} for item in config["tables"]
    ]
    return config


def row_to_record(row, field_mappings):
    """Convert a database row into the record to be transmitted to Airtable."""
    row = dict(row)
    result = {}
    for airtable_name, query_names in field_mappings.items():
        query_names = query_names or []
        for query_name in query_names:
            value = row.get(query_name)
            if value is not None:
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                elif isinstance(value, datetime.datetime):
                    value = value.strftime("%m/%d/%Y %H:%M:%S")
                elif isinstance(value, datetime.date):
                    value = value.isoformat("%m/%d/%Y")
                elif (
                    isinstance(value, str)
                    and value.strip()
                    and query_name.endswith("_list")
                ):
                    value = [item.strip() for item in value.split("|")]
                result[airtable_name] = value
                break
    return result


def run_query(connection, table_info):
    table_name = table_info["table"]
    return connection.execute(f"select * from identifier('{table_name}')").fetchall()


def send_records(airtable, airtable_table, result, table_info, field_mappings):
    name = table_info["table"]
    log.info(
        "sending records to airtable",
        count=len(result),
        source=name,
        destination=airtable_table,
    )
    update = table_info.get("update", False)
    records = [row_to_record(row, field_mappings) for row in result]
    if records:
        airtable.write(records, airtable_table, update=update)
    action = "updated" if update else "inserted"
    log.info(
        f"{action} record(s) in airtable",
        count=len(result),
        source=name,
        destination=airtable_table,
    )


def run(config_file, max_workers=None):
    engine = get_engine()
    config = read_config(config_file)
    airtable = AirtableClient(
        config["airtable_base_id"], os.environ["AIRTABLE_API_KEY"]
    )
    airtable_table = config["airtable_table_name"]
    metadata = airtable.get_metadata()
    field_mappings = get_field_mappings(metadata[airtable_table].keys())
    errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # We want to run the queries concurrently (most of our time will be spent
        # waiting for Snowflake to return results), but then emit the data to
        # Airtable serially (to make managing the requests limit simpler)
        futures = {
            executor.submit(run_query, engine, table_info): table_info
            for table_info in config["tables"]
        }
        for future in concurrent.futures.as_completed(futures):
            table_info = futures[future]
            table_name = table_info["table"]
            try:
                result = future.result()
            except Exception as exc:
                log.error("error querying table", table=table_name, error=exc)
                errors.append((table_info, exc))
            else:
                send_records(
                    airtable,
                    airtable_table,
                    result,
                    table_info,
                    field_mappings,
                )
    if errors:
        error_tables = [t["table"] for (t, _) in errors]
        raise Exception(f"Errors running table(s): {', '.join(error_tables)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--max-workers", type=int, default=None)

    args = parser.parse_args()

    run(args.config_file, max_workers=args.max_workers)
