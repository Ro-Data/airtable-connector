# airtable_connector

Load [Airtable](https://airtable.com/) tables into your [Snowflake](https://www.snowflake.com/) database, and send rows from Snowflake to Airtable.

## Setup

### Airtable API key

[Generate an API key](https://support.airtable.com/hc/en-us/articles/219046777-How-do-I-get-my-API-key-) and export it in your environment as `AIRTABLE_API_KEY`

### Snowflake credentials

The following environment variables are mandatory:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_DATABASE`
- Either `SNOWFLAKE_PASSWORD`, or `SNOWFLAKE_PRIVATE_KEY_PATH` plus `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

The following are optional:

- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_LOGIN` (if your login name is different than your user name)

## Usage

### Load from Airtable into Snowflake

To load an Airtable table into Snowflake:

```bash
python -m airtable_connector.load \
    --base-id="YOUR_BASE_ID" \
    --table-name="YOUR_AIRTABLE_TABLE_NAME" \
    --destination="YOUR_QUALIFIED_DESTINATION_TABLE"
```

### Emit data from Snowflake to Airtable

To emit rows from a Snowflake table to Airtable:

```bash
python -m airtable_connector.send --config-file=path/to/your/config.yml
```

The config file format looks like:

```yaml
airtable_base_id: "YOUR_BASE_ID"
airtable_table_name: "YOUR_AIRTABLE_TABLE_NAME"
tables:
  - db.schema.table_1
  - db.schema.table_2
```

You can also have tables which are used to update existing records in Airtable. In that case, the configuration looks like:

```yaml
airtable_base_id: "YOUR_BASE_ID"
airtable_table_name: "YOUR_AIRTABLE_TABLE_NAME"
tables:
  - table: db.schema.table_1
    update: false
  - table: db.schema.table_1_update
    update: true
```

Tables configured with `update: true` must have an `id` column, which is the Airtable-assigned ID of the record to update (likely obtained by having loaded that Airtable data into Snowflake)
