#!/usr/bin/env python3
import datetime
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import requests


REQUIRED_CONFIG_KEYS = ["start_date", "pocket_username", "pocket_consumer_key", "pocket_access_token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        if not filename.endswith('.json'):
            continue
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

        args = {
            "consumer_key": config["pocket_consumer_key"],
            "access_token": config["pocket_access_token"],
            "sort": "oldest",
            "state": "all",
            "detailType": "complete",
        }

        if stream.tap_stream_id in state:
            args["since"] = state[stream.tap_stream_id]
        elif config["start_date"] is not None:
            args["since"] = datetime.datetime.fromisoformat(config["start_date"].rstrip('Z')).timestamp()

        offset = 0
        retries = 5
        page_size = 100

        while True:
            args.update({
                "count": page_size,
                "offset": offset,
            })

            response = requests.get("https://getpocket.com/v3/get", args)
            if response.status_code == 503 and retries < 5:
                LOGGER.info("Got a 503, retrying...")
                retries += 1
                time.sleep(retries * 1000)
                continue
            else:
                retries = 0

            response.raise_for_status()
            page = response.json()
            next_since = page["since"]

            items = []
            ilist = page.get("list", [])
            if isinstance(ilist, dict):
                ilist = ilist.values()

            for row in ilist:
                item = {}
                for k in ["given_title", "given_url", "resolved_title", "resolved_url", "time_added", "time_updated"]:
                    item[k] = row.get(k, None)
                for k in ["item_id", "status", "favourite"]:
                    item[k] = int(row.get(k, 0))
                items.append(item)

            if len(items):
                singer.write_records(stream.tap_stream_id, items)

            singer.write_state({stream.tap_stream_id: next_since})

            if not len(items):
                break

            offset += page_size

    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
