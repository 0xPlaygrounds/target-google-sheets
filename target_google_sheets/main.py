import json
import logging
import sys
import types
import typing
from argparse import ArgumentParser
from collections import defaultdict
from io import TextIOWrapper
from pathlib import Path
from typing import Iterable

import gspread
import jsonschema
import singer

from .exceptions import MessageNotRecognized, OverflowedSink, SchemaNotFound
from .models import SingerData, TargetGoogleSheetConfig
from .utils import DEFAULT_CREDENTIALS_PATH, get_credentials

logging.getLogger("gspread").setLevel(logging.WARNING)

#: Default size for worksheet creation
WORKSHEET_DEFAULT_ROWS, WORKSHEET_DEFAULT_COLS = 100, 20

#: Sink Settings (in rows)
DEFAULT_SINK_SIZE = 50
SINK_LIMIT_INCREMENT = 20
MAX_SINK_LIMIT = 250


class GoogleSheetsSink:
    """A RECORD row sink to hold batches of rows before sending to google sheets

    This class defines a sink object which will batch rows up before sending a single
      Gspread API request. This allows us to make significantly less requests
      making the singer target more efficient, and less taxing on API limits.

    If a request fails, the sink increases it's size by a set increment, if requests
      continue to fail, the max sink limit will be reached causing an overflow exception.

    Each stream will have it's own "sink" so this class acts as a collection of sinks.

    Constants:
      - DEFAULT_SINK_SIZE: Default size of sink
      - SINK_LIMIT_INCREMENT: How much the sink should grow
      - MAX_SINK_LIMIT: Max size of sink before overflowing
    """

    sinks: defaultdict[str, list[list]]
    limit: defaultdict[str, int]
    worksheets: dict[str, gspread.Spreadsheet]

    def __init__(self, spreadsheet: gspread.Spreadsheet):
        self.sinks = defaultdict(list)
        self.limit = defaultdict(lambda: DEFAULT_SINK_SIZE)
        self.spreadsheet = spreadsheet
        self.worksheets = {}

    def get_or_create_sheet(self, name: str, record: dict):
        """Retrieves a sheet from the gspread API

        If a sheet name is not found, then the method will produce a new sheet.
        It will also prefil the first row with column information curated from a
          singer record/row.
        """

        name = name.replace(":", "_")

        try:
            return self.spreadsheet.worksheet(name)

        except gspread.WorksheetNotFound:
            singer.log_info(f"Creating new worksheet: {name}")
            sh = self.spreadsheet.add_worksheet(
                title=name, rows=WORKSHEET_DEFAULT_ROWS, cols=WORKSHEET_DEFAULT_COLS
            )
            sh.append_row(list(record.keys()), value_input_option="RAW")
            return sh

    def add(self, stream: str, record: dict):
        """Add a record to the sink for a specific singer stream

        Checks if the stream's sink is overflowed and drains it if so.
        """
        self.sinks[stream].append(list(record.values()))

        if stream not in self.worksheets:
            self.worksheets[stream] = self.get_or_create_sheet(stream, record)

        self.check(stream)

    def check(self, stream):
        """Drains sink after checking for overflow"""

        if len(self.sinks[stream]) > self.limit[stream]:
            self.drain(stream)

    def drain(self, stream: str):
        sink = self.sinks[stream]

        try:
            sheet = self.worksheets[stream]
            sheet.append_rows(sink, value_input_option="RAW")
            singer.log_info(f"Sink limit hit, draining {len(sink)} rows")
            self.sinks[stream] = []

        except gspread.exceptions.APIError as err:
            if not err.response.status_code == 429:
                raise err

            if self.limit[stream] > MAX_SINK_LIMIT:
                raise OverflowedSink(f"Max sink size of {self.limit[stream]} reached.")

            singer.log_warning(
                f"Google Sheets API Quote reached. Increasing size of sink {stream} temporarily.."
            )
            self.limit[stream] += SINK_LIMIT_INCREMENT

    def drain_all(self):
        """Drains all sinks (that have rows)

        Generally used after winding down the target.
        """

        for stream, sink in self.sinks.items():
            if sink:
                self.drain(stream)

        singer.log_info("All sinks drained!")


def parser():
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-c", "--config", help="Config file", required=True)

    return arg_parser


def read_stdin() -> Iterable[str]:
    """Uses TextIOWrapper to yield lines of strings"""

    yield from TextIOWrapper(sys.stdin.buffer, encoding="utf-8")


def output_state(state: dict | None):
    """Write JSON to stdout directly to produce state"""

    if state is None:
        return

    raw = json.dumps(state)
    singer.log_debug(f"Outputting State: {raw}")
    sys.stdout.write(raw + "\n")
    sys.stdout.flush()


def flatten_record(record: typing.MutableMapping) -> dict:
    """Recursively flatten records to fit inside a tabular Google Sheet"""

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
    """Main match statement to parse and understand the Singer messages"""

    match msg:
        case singer.SchemaMessage():
            data.schemas[msg.stream] = msg.schema
            data.key_properties[msg.stream] = msg.key_properties

            return None

        case singer.StateMessage():
            singer.log_debug(f"State set to: {msg.value}")
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
    """Iteratively processes the messages from the message_stream

    After exhausting stream, drain all
    """

    singer_data: SingerData = types.SimpleNamespace(
        schemas={}, state={}, key_properties={}
    )

    sink = GoogleSheetsSink(sh)

    state = None
    for raw_msg in message_stream:
        try:
            msg = singer.parse_message(raw_msg)
        except json.decoder.JSONDecodeError:
            singer.log_error("Parsing failed for message:\n {raw}")
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

    credential_path = config.get("credentials_path", DEFAULT_CREDENTIALS_PATH)
    spreadsheet = get_spreadsheet(config["spreadsheet_url"], Path(credential_path))

    process_stream(spreadsheet, message_stream)
    singer.log_info("Target has consumed all streams to completion")


def get_spreadsheet(url: str, crendentials: Path) -> gspread.Spreadsheet:
    """Gets spreadsheet by url

    raises SpreadsheetNotFound
    """

    try:
        credentials = get_credentials(crendentials)  # can raise error
        gc = gspread.service_account(credentials)
        return gc.open_by_url(url)

    except gspread.SpreadsheetNotFound:
        raise gspread.SpreadsheetNotFound(
            f"Spreadsheet not found, url: {url}"
        ) from None


def get_config(args):
    """Gets config from args"""
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
