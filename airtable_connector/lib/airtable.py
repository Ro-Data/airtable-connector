import itertools
import math
import os
import time
import urllib.parse

import requests
import structlog

log = structlog.get_logger()


class AirtableIteratorError(Exception):
    pass


class AirtableClient:
    def __init__(
        self,
        base_id,
        api_key=None,
        write_chunk_size=10,
        requests_per_second_limit=5,
    ):
        api_key = api_key or os.environ["AIRTABLE_API_KEY"]
        self.base_id = base_id
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}"})
        self.write_chunk_size = write_chunk_size
        self.requests_per_second_limit = requests_per_second_limit
        self.metadata = None

    def request(self, method, *args, **kwargs):
        """Make a request to the Airtable API.

        Attempt to handle the requests-per-second limit, including HTTP 429 (rate limit)
        responses transparently. Raise an exception for any other error, or if still
        receiving HTTP 429 after three retries.
        """
        headers = (
            {"Content-Type": "application/json"}
            if method.lower() in ("put", "post", "patch")
            else {}
        )
        headers.update(kwargs.pop("headers", {}))
        # Airtable says to wait 30 seconds before retrying. Start there and
        # increase 50% on each retry, up to 3 times
        retries = 0
        wait = 30
        while True:
            response = self.session.request(method, *args, headers=headers, **kwargs)
            if response.status_code != 429 or retries == 3:
                break
            retries += 1
            log.warn(
                "http 429 response from airtable, retry after wait",
                retry=retries,
                wait=wait,
            )
            time.sleep(wait)
            wait = math.ceil(wait * 1.5)
        response.raise_for_status()
        # Stay under the requests per second limit
        time.sleep(1 / self.requests_per_second_limit)
        return response

    def get_metadata(self, force_refresh=False):
        """Return a `dict` of {table_name: {field_name: field_type}}."""
        if (self.metadata is None) or force_refresh:
            response = self.request(
                "get", f"https://api.airtable.com/v0/meta/bases/{self.base_id}/tables"
            )
            data = response.json()
            self.metadata = {
                table["name"]: {
                    field["name"]: field["type"] for field in table.get("fields", [])
                }
                for table in data.get("tables", [])
            }
        return self.metadata

    def iter_chunks(self, table_name):
        """Yield 100-record chunks (the API's maximum size) from `table_name`.

        Raise an `AirtableIteratorError` if Airtable signals an
        `LIST_RECORDS_ITERATOR_NOT_AVAILABLE` error. In this case, the caller should
        discard any records read to that point and restart reading from the beginning
        (e.g. via a new call to `iter_chunks`).
        """
        table_name = urllib.parse.quote(table_name)
        table_url = f"https://api.airtable.com/v0/{self.base_id}/{table_name}"
        offset = None
        while True:
            url = f"{table_url}?offset={offset}" if offset else table_url
            try:
                response = self.request("get", url)
            except requests.exceptions.HTTPError as exc:
                if is_iterator_error(exc):
                    raise AirtableIteratorError() from exc
                else:
                    raise
            else:
                data = response.json()
                yield data.get("records", [])
                offset = data.get("offset", None)
                if offset is None:
                    break

    def read(self, table_name):
        """Return a full list of records read from `table_name`.

        Automatically retry 3 times in the event of a
        `LIST_RECORDS_ITERATOR_NOT_AVAILABLE` from Airtable; then raise if still
        unsuccessful.
        """
        restarts = 0
        while True:
            try:
                return list(itertools.chain.from_iterable(self.iter_chunks(table_name)))
            except AirtableIteratorError:
                if restarts == 3:
                    raise
                else:
                    restarts += 1
                    log.warn(
                        "airtable list records iterator not available, restarting",
                        restart=restarts,
                    )

    def write(self, records, table_name, update=False):
        """Write `records` to `table_name`.

        If `update` is true, submit as a `PATCH` request to existing records. In this
        case, every record must have an `id` (the Airtable-assigned ID of the record to
        be updated). Otherwise, append the records to the table.
        """
        table_name = urllib.parse.quote(table_name)
        url = f"https://api.airtable.com/v0/{self.base_id}/{table_name}"
        if update:
            method, build_payload = "patch", build_update_payload
        else:
            method, build_payload = "post", build_append_payload
        for chunk in chunked(records, self.write_chunk_size):
            payload = build_payload(chunk)
            self.request(method, url, json=payload)

    def append(self, records, table_name):
        """Append `records` to `table_name`."""
        self.write(records, table_name, update=False)

    def update(self, records, table_name):
        """Update existing `records` in `table_name`.

        Every record must have an `id` (the Airtable-assigned ID of the record to
        be updated)
        """
        self.write(records, table_name, update=True)


def build_append_payload(records):
    return {
        "records": [{"fields": record} for record in records],
        "typecast": True,
    }


def build_update_payload(records):
    return {
        "records": [{"id": record.pop("id"), "fields": record} for record in records],
        "typecast": True,
    }


def is_iterator_error(exc):
    """Return true if `exc` (an `HTTPError`) signals we must restart iteration."""
    if exc.response.status_code == 422:
        try:
            data = exc.response.json()
        except Exception:
            pass
        else:
            if "LIST_RECORDS_ITERATOR_NOT_AVAILABLE" in data.get("error", ""):
                return True
    return False


def chunked(seq, n):
    if not isinstance(seq, list):
        seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i : i + n]
