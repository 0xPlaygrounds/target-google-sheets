from typing import Protocol, TypedDict


class TargetGoogleSheetConfig(TypedDict):
    """Simple type definition for the target config"""

    spreadsheet_url: str


class SingerData(Protocol):
    """Simplified representation of singer data"""

    schemas: dict
    key_properties: dict
    state: dict
