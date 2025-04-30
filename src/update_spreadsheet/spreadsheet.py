import pandas as pd
from io import BytesIO
import gspread
from .config import SPREADSHEET_ID, get_credentials

gc = gspread.authorize(get_credentials())
ss = gc.open_by_key(SPREADSHEET_ID)
sheet = ss.worksheet("Sheet1")


def fetch_existing() -> pd.DataFrame:
    recs = sheet.get_all_records()
    return pd.DataFrame(recs) if recs else pd.DataFrame()


def append_rows(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    rows = df.astype(str).values.tolist()
    if not sheet.row_values(1):
        sheet.append_row(df.columns.tolist(), value_input_option="RAW")
    sheet.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def fetch_new_rows(excel_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(excel_bytes))
    df["Timestamp"] = pd.to_datetime(
        df["Data"] + " " + df["Hora"], dayfirst=True)
    df = df.sort_values("Timestamp")
    existing = fetch_existing()

    if not existing.empty:
        existing_ts = pd.to_datetime(
            existing["Data"] + " " + existing["Hora"], dayfirst=True)
        last_ts = existing_ts.max()
        df = df[df["Timestamp"] > last_ts]
    return df
