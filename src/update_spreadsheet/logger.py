import logging
from datetime import datetime, timezone
import gspread
import gspread.exceptions as gspread_ex
from .config import SPREADSHEET_ID, get_credentials

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def get_log_worksheet():
    gc = gspread.authorize(get_credentials())
    ss = gc.open_by_key(SPREADSHEET_ID)
    try:
        return ss.worksheet("Logs")
    except gspread_ex.WorksheetNotFound:
        ws = ss.add_worksheet(title="Logs", rows="100", cols="3")
        ws.append_row(["RunTimestamp", "Status", "Message"],
                      value_input_option="RAW")
        return ws


def record_log(status: str, message: str):
    ws = get_log_worksheet()
    ts = datetime.now(timezone.utc).isoformat()
    ws.append_row([ts, status, message], value_input_option="RAW")
    logging.info("Log recorded: %s - %s", status, message)
