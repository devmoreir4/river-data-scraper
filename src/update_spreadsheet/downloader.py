import requests
import logging
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from .config import ALERT_URL, LINK_TEXT


@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=10, max=60))
def download_excel(url: str = ALERT_URL, link_text: str = LINK_TEXT) -> bytes:
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    link = soup.find("a", string=link_text)

    if not link:
        raise RuntimeError(f"Link '{link_text}' not found.")

    href = link["href"]
    excel_url = href if href.startswith(
        "http") else requests.compat.urljoin(url, href)

    logging.info("Download Excel from %s", excel_url)
    r2 = requests.get(excel_url, timeout=10)
    r2.raise_for_status()
    return r2.content
