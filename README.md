# target-gspread

## Quick Start
1. Follow the guides from the [Google Sheet API Docs](https://developers.google.com/sheets/api/quickstart/python) to create a new service account
   1. You should follow this [wizard](https://console.developers.google.com/start/api?id=sheets.googleapis.com) ideally.
2. Enable the [Google Drive API](https://console.developers.google.com/apis/api/drive.googleapis.com/overview?) (This link should automatically take you to your project).
3. Create a new credentials key for your **service account** and download it to the `.secrets` folder in your project.

## Configuration
See `config.sample.json` for more information.

```json
{
    "spreadsheet_url": "https://..."
}
```