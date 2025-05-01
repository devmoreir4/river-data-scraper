import logging
from datetime import datetime, timedelta, timezone
import pandas as pd

from .config import GITHUB_EVENT, UPDATE_INTERVAL_DAYS, validate_env
from .downloader import download_excel
from .spreadsheet import fetch_new_rows, append_rows
from .logger import record_log, get_log_worksheet


def should_skip() -> bool:
    if GITHUB_EVENT != "schedule":
        return False

    ws = pd.DataFrame(get_log_worksheet().get_all_records())
    if ws.empty:
        return False
    last_success = ws[ws.Status == "success"].RunTimestamp.max()
    last_dt = datetime.fromisoformat(last_success)
    return datetime.now(timezone.utc) < last_dt + timedelta(days=UPDATE_INTERVAL_DAYS)


def run():
    validate_env()
    if should_skip():
        msg = f"Skipping: less than {UPDATE_INTERVAL_DAYS} days since last success"
        logging.info(msg)
        record_log("skipped", msg)
        return

    try:
        excel = download_excel()
        new_df = fetch_new_rows(excel)
        added = append_rows(new_df)
        msg = f"Success – {added} records added"
        logging.info(msg)
        record_log("success", msg)
    except Exception as e:
        msg = str(e)
        logging.error("Execution error: %s", msg)
        record_log("error", msg)
        raise


if __name__ == "__main__":
    run()
