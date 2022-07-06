# target-google-sheets

## Install

We recommend installing all singer taps and targets into their own environments as per the [Singer FAQ](https://github.com/singer-io/getting-started/blob/master/docs/FAQ.md#how-do-i-prevent-dependency-conflicts-between-my-tap-and-target).

```bash
pip install https://github.com/0xPlaygrounds/target-google-sheets
```

## Setup Google Sheets
1. Follow the guides from the [Google Sheet API Docs](https://developers.google.com/sheets/api/quickstart/python) to create a new service account
   1. You should follow this [wizard](https://console.developers.google.com/start/api?id=sheets.googleapis.com) ideally.
2. Enable the [Google Drive API](https://console.developers.google.com/apis/api/drive.googleapis.com/overview?) (This link should automatically take you to your project).
3. Create a new credentials key for your **service account**, download it to the `.secrets`, and rename it to `credentials.json`. Alternatively, you can place it whereever you want and define the path to it via the config.
> TODO: Add ability to parse from environment variables

## Invocation

```bash
tap-fixerio | target-google-sheets -c sheets.json
```

## Configuration
See `config.sample.json` for more information. The bare mininum is as follows:

```json
{
    "spreadsheet_url": "https://..."
}
```

## About

Playgrounds is building an accessible, robust, and multi-chain data stack for Web3. Follow us and stay in touch on our journey to revolutionize data access.
