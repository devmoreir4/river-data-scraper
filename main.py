import os
import json
import logging
from io import BytesIO
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import gspread.exceptions as gspread_ex

load_dotenv()

DEBUG = os.getenv("DEBUG", "0") == "1"
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = "Sheet1"
LOG_SHEET = "Logs"
ALERT_URL = "http://alertadecheias.inea.rj.gov.br/alertadecheias/214109520.html"
LINK_TEXT = os.getenv("LINK_TEXT", "Exportar para Excel.") # text to find in the page

# schedule vs manual
GITHUB_EVENT = os.getenv("GITHUB_EVENT_NAME", "").lower()

if not SERVICE_ACCOUNT_JSON or not SPREADSHEET_ID:
    logging.error(
        "Defina SERVICE_ACCOUNT_JSON e SPREADSHEET_ID no .env/secrets.")
    exit(1)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

try:
    if DEBUG:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_JSON,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
    else:
        info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
except Exception as e:
    logging.error("Falha ao carregar credenciais: %s", e)
    exit(1)

gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key(SPREADSHEET_ID)
sheet = spreadsheet.worksheet(SHEET_NAME)

# logs
try:
    log_ws = spreadsheet.worksheet(LOG_SHEET)
except gspread_ex.WorksheetNotFound:
    log_ws = spreadsheet.add_worksheet(title=LOG_SHEET, rows="100", cols="3")
    log_ws.append_row(["RunTimestamp", "Status", "Message"],
                      value_input_option="RAW")


def record_log(status: str, message: str):
    ts = datetime.now(timezone.utc).isoformat()
    log_ws.append_row([ts, status, message], value_input_option="RAW")


@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=10, max=60))
def download_excel(url: str, link_text: str = LINK_TEXT) -> bytes:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    link = soup.find("a", string=link_text)
    if not link:
        raise Exception(f"Link '{link_text}' não encontrado.")
    href = link["href"]
    excel_url = href if href.startswith(
        "http") else requests.compat.urljoin(url, href)
    logging.info("Baixando Excel de %s", excel_url)
    r = requests.get(excel_url, timeout=10)
    r.raise_for_status()
    return r.content


def fetch_new_rows(excel_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(excel_bytes))
    df["Timestamp"] = pd.to_datetime(
        df["Data"] + " " + df["Hora"], dayfirst=True
    )
    df = df.sort_values("Timestamp")

    existing = pd.DataFrame(sheet.get_all_records())
    if not existing.empty:
        existing_ts = pd.to_datetime(
            existing["Data"] + " " + existing["Hora"], dayfirst=True
        )
        last_ts = existing_ts.max()
        df = df[df["Timestamp"] > last_ts]
    return df


def append_to_sheet(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    cols = [c for c in df.columns if c != "Timestamp"] + ["Timestamp"]
    df = df[cols]
    rows = df.astype(str).values.tolist()

    # add header
    if not sheet.row_values(1):
        sheet.append_row(df.columns.tolist(), value_input_option="RAW")
    sheet.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def job():
    # in scheduling, check last successful run
    if GITHUB_EVENT == "schedule":
        logs = pd.DataFrame(log_ws.get_all_records())
        if not logs.empty:
            success_logs = logs[logs["Status"] == "success"]
            if not success_logs.empty:
                last = success_logs["RunTimestamp"].max()
                last_dt = datetime.fromisoformat(last)
                if datetime.now(timezone.utc) < last_dt + timedelta(days=10):
                    msg = "Pulando execução: menos de 10 dias desde último sucesso"
                    logging.info(msg)
                    record_log("skipped", msg)
                    return
    try:
        data = download_excel(ALERT_URL)
        new_count = fetch_new_rows(data)
        added = append_to_sheet(new_count)
        msg = f"Sucesso - {added} registros adicionados"
        logging.info(msg)
        record_log("success", msg)
    except Exception as e:
        err = str(e)
        logging.error("Erro na execução: %s", err)
        record_log("error", err)


if __name__ == "__main__":
    job()
