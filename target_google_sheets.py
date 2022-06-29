import json
import logging
import sys
import types
import typing
from argparse import ArgumentParser
from collections import defaultdict
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Iterable, Protocol, TypedDict
from unittest.mock import DEFAULT

import gspread
import jsonschema
import singer

logging.getLogger("gspread").setLevel(logging.WARNING)
logger = singer.get_logger()

SECRETS = Path(".secrets")
CREDENTIALS = SECRETS / "service_account.json"

WORKSHEET_DEFAULT_ROWS, WORKSHEET_DEFAULT_COLS = 100, 20

# Sink Settings (in rows)
DEFAULT_SINK_SIZE = 50
SINK_LIMIT_INCREMENT = 20
MAX_SINK_LIMIT = 250


class TargetException(Exception):
    ...


class MessageNotRecognized(TargetException):
    ...


class SchemaNotFound(TargetException):
    ...


class OverflowedSink(TargetException):
    ...


class TargetGoogleSheetConfig(TypedDict):
    spreadsheet_url: str


class SingerData(Protocol):
    schemas: dict
    key_properties: dict
    state: dict


class GoogleSheetsSink:
    sinks: defaultdict[str, list[list]]
    limit: defaultdict[str, int]
    worksheets: dict[str, gspread.Spreadsheet]

    def __init__(self, spreadsheet: gspread.Spreadsheet):
        self.sinks = defaultdict(list)
        self.limit = defaultdict(lambda: DEFAULT_SINK_SIZE)
        self.spreadsheet = spreadsheet
        self.worksheets = {}

    def get_or_create_sheet(self, name: str, record: dict):
        name = name.replace(":", "_")
        try:
            return self.spreadsheet.worksheet(name)

        except gspread.WorksheetNotFound:
            logger.info(f"Creating new worksheet: {name}")
            sh = self.spreadsheet.add_worksheet(
                title=name, rows=WORKSHEET_DEFAULT_ROWS, cols=WORKSHEET_DEFAULT_COLS
            )
            sh.append_row(list(record.keys()), value_input_option="RAW")
            return sh

    def add(self, stream: str, record: dict):
        self.sinks[stream].append(list(record.keys()))

        if stream not in self.worksheets:
            self.worksheets[stream] = self.get_or_create_sheet(stream, record)

        self.check(stream)

    def check(self, stream):
        if len(self.sinks[stream]) > self.limit[stream]:
            self.drain(stream)

    def drain(self, stream: str):
        sink = self.sinks[stream]

        try:
            sheet = self.worksheets[stream]
            sheet.append_rows(sink, value_input_option="RAW")
            logger.info(f"Sink limit hit, draining {len(sink)} rows")
            self.sinks[stream] = []

        except gspread.exceptions.APIError as err:
            if not err.response.status_code == 429:
                raise err

            if self.limit[stream] > MAX_SINK_LIMIT:
                raise OverflowedSink(
                    f"Max sink size of {self.limit[stream]} reached."
                )

            logger.warning(
                f"Google Sheets API Quote reached. Increasing size of sink {stream} temporarily.."
            )
            self.limit[stream] += SINK_LIMIT_INCREMENT
    
    def drain_all(self):
        for stream, sink in self.sinks.items():
            if sink:
                self.drain(stream)
        
        logger.info("All sinks drained!")


def parser():
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-c", "--config", help="Config file", required=True)

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


def process_message(msg: singer.Message, data: SingerData) -> dict[str, dict] | None:
    match msg:
        case singer.SchemaMessage():
            data.schemas[msg.stream] = msg.schema
            data.key_properties[msg.stream] = msg.key_properties

            return None

        case singer.StateMessage():
            logger.debug(f"State set to: {msg.value}")
            state: dict = msg.value

            return {"state": state}

        case singer.RecordMessage():
            if msg.stream not in data.schemas:
                raise SchemaNotFound(
                    f"Record for stream {msg.stream} was found before the cooresponding schema was recorded"
                )

            jsonschema.validate(msg.record, data.schemas[msg.stream])
            flattened_record = flatten_record(msg.record)

            return {"record": flattened_record}

        case _:
            raise MessageNotRecognized(
                f"Message type {type(msg)} not recognized\n{msg}"
            )


def process_stream(sh: gspread.Spreadsheet, message_stream: Iterable[str]):
    singer_data: SingerData = types.SimpleNamespace(
        schemas={}, state={}, key_properties={}
    )

    sink = GoogleSheetsSink(sh)

    state = None
    for raw_msg in message_stream:
        try:
            msg = singer.parse_message(raw_msg)
        except json.decoder.JSONDecodeError:
            logger.error("Parsing failed for message:\n {raw}")
            raise

        match process_message(msg, singer_data):
            case {"state": state}:
                ...

            case {"record": record}:
                sink.add(msg.stream, record)

    sink.drain_all()
    output_state(state)


def main():
    args = parser().parse_args()
    config = get_config(args)
    message_stream = read_stdin()

    spreadsheet = get_spreadsheet(config["spreadsheet_url"])

    process_stream(spreadsheet, message_stream)
    logger.info("Target has consumed all streams to completion")


def get_spreadsheet(url: str) -> gspread.SpreadsheetNotFound:
    try:
        gc: gspread.Client = gspread.service_account(CREDENTIALS)
        sh: gspread.Spreadsheet = gc.open_by_url(url)
    except gspread.SpreadsheetNotFound:
        raise gspread.SpreadsheetNotFound(
            f"Spreadsheet not found, url: {url}"
        ) from None
    return sh


def get_config(args):
    try:
        content = Path(args.config).read_text()
        config: TargetGoogleSheetConfig = json.loads(content)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Configuration file not found: '{args.config}'"
        ) from None
    except json.JSONDecodeError:
        raise json.JSONDecodeError(
            f"Configuration file at '{args.config}' is improper json"
        ) from None
    return config


if __name__ == "__main__":
    main()
