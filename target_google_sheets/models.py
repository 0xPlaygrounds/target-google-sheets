from typing import Protocol, TypedDict

from typing_extensions import NotRequired


class SinkConfig(TypedDict):
    default_sink_size: NotRequired[int]
    sink_limit_increment: NotRequired[int]
    max_sink_limit: NotRequired[int]


class TargetGoogleSheetConfig(TypedDict):
    """Simple type definition for the target config"""

    spreadsheet_url: str
    credentials_path: NotRequired[str]
    sink: NotRequired[SinkConfig]


class SingerData(Protocol):
    """Simplified representation of singer data"""

    schemas: dict
    key_properties: dict
    state: dict
