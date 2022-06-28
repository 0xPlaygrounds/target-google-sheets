import json
import types
from typing import Any, Iterable, Protocol, TypedDict
import typing
import gspread
from pathlib import Path

from argparse import ArgumentParser

import jsonschema

from io import TextIOWrapper
import sys

import logging
import singer

logging.getLogger('gspread').setLevel(logging.WARNING)
logger = singer.get_logger()

SECRETS = Path(".secrets")
CREDENTIALS = SECRETS / "service_account.json"

WORKSHEET_DEFAULT_ROWS, WORKSHEET_DEFAULT_COLS = 100, 20

class TargetGspreadException(Exception):
    ...

class MessageNotRecognized(TargetGspreadException):
    ...

class SchemaNotFound(TargetGspreadException):
    ...


class TargetGSpreadConfig(TypedDict):
    spreadsheet_url: str


class SingerData(Protocol):
    schemas: dict
    key_properties: dict
    state: dict


def parser():
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='Config file', required=True)

    return arg_parser


def read_stdin() -> Iterable[str]:
    """Uses TextIOWrapper to yield lines of strings"""

    yield from TextIOWrapper(sys.stdin.buffer, encoding="utf-8")


def output_state(state: dict | None):
    if state is None:
        return
    
    raw = json.dumps(state)
    logger.debug(f"Outputting State: {raw}")
    sys.stdout.write(raw + "\n")
    sys.stdout.flush()


def flatten_record(record: typing.MutableMapping) -> dict:
    def items():
        for key, value in record.items():
            match value:
                case typing.MutableMapping:
                    for sub_key, sub_value in flatten_record(value).items():
                        yield f"{key}.{sub_key}", sub_value

                case _:
                    yield key, value
    
    return dict(items())
            

def process_message(msg: str, data: SingerData) -> dict[str, dict] | None:
    match msg:
        case singer.SchemaMessage:
            data.schemas[msg.stream] = msg.schema
            data.key_properties[msg.stream] = msg.key_properties

            return None
        
        case singer.StateMessage:
            logger.debug(f"State set to: {msg.value}")
            state: dict = msg.value

            return {"state": state}
            
        case singer.RecordMessage:
            if msg.stream not in data.schemas:
                raise SchemaNotFound(f"Record for stream {msg.stream} was found before the cooresponding schema was recorded")
            
            jsonschema.validate(schema := data.schemas[msg.stream])
            flattened_record = flatten_record(msg.record)

            return {"record": flattened_record}
        
        case _:
            raise MessageNotRecognized(f"Message {msg} not recognized")


def process_stream(sh: gspread.Spreadsheet, stream: Iterable[str]):
    singer_data: SingerData = types.SimpleNamespace(schemas={}, state={}, key_properties={})

    state = None
    for raw_msg in stream:
        try:
            msg = singer.parse_message(raw_msg)
        except json.decoder.JSONDecodeError:
            logger.error("Parsing failed for message:\n {raw}")
            raise

        match process_message(msg, singer_data):
            case {"state": state}:
                ...
            
            case {"record": record}:
                worksheet = get_or_create_sheet(sh, msg.stream, record)

                list_of_rows = [list(row) for row in zip(*record.values())]
                
                # make sure we prepend the header with the column names
                if not worksheet.row_values(1):
                    list_of_rows.prepend(list(record.keys()))

                worksheet.append_rows(
                    list_of_rows, value_input_option="RAW"
                )

    output_state(state)


def get_or_create_sheet(spreadsheet: gspread.Spreadsheet, name: str, record: dict):
    try:
        return spreadsheet.worksheet(name)

    except gspread.WorksheetNotFound:
        logger.info(f"Creating new worksheet: {name}")
        return spreadsheet.add_worksheet(title=name, rows=WORKSHEET_DEFAULT_ROWS, cols=WORKSHEET_DEFAULT_COLS)


def main():
    args = parser().parse_args()
    config = get_config(args)
    stream = read_stdin()

    gc: gspread.Client = gspread.service_account(CREDENTIALS)
    sh: gspread.Spreadsheet = gc.worksheet(config["spreadsheet_url"])

    process_stream(sh, stream)
    logger.info("Target has consumed all streams to completion")


def get_config(args):
    try:
        content = Path(args.config).read_text()
        config: TargetGSpreadConfig = json.load(content)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: '{args.config}'") from None
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"Configuration file at '{args.config}' is improper json") from None
    return config


if __name__ == "__main__":
    main()
