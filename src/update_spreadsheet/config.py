import os
import json
import logging
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

DEBUG = os.getenv("DEBUG", "0") == "1"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
ALERT_URL = "http://alertadecheias.inea.rj.gov.br/alertadecheias/214109520.html"
LINK_TEXT = "Exportar para Excel."
GITHUB_EVENT = os.getenv("GITHUB_EVENT_NAME", "").lower()
UPDATE_INTERVAL_DAYS = int(os.getenv("UPDATE_INTERVAL_DAYS", 8))


def get_credentials() -> Credentials:
    if DEBUG:
        return Credentials.from_service_account_file(
            SERVICE_ACCOUNT_JSON,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    info = json.loads(SERVICE_ACCOUNT_JSON)
    return Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )


def validate_env():
    if not SERVICE_ACCOUNT_JSON or not SPREADSHEET_ID:
        logging.error(
            "Set SERVICE_ACCOUNT_JSON and SPREADSHEET_ID in .env/secrets.")
        raise SystemExit(1)
