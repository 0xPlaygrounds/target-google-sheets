from pathlib import Path

#: Default credentials path (both `~` and `pwd`)
DEFAULT_CREDENTIALS_PATH = Path(".secrets/google_sheets.json")


def get_credentials(credentials_path) -> Path:
    """Gets google_sheets credentials from either PWD or $HOME"""

    if credentials_path.exists():
        return credentials_path

    if (credentials_path := Path.home() / credentials_path).exists():
        return credentials_path

    raise FileNotFoundError(
        "Failed to locate Google Sheets credentials\n"
        "Follow the README instructions and place credentials in a `.secrets/google_sheets.json"
        " either in your local directory or in your $HOME directory."
    )
