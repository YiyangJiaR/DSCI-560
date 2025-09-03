from bs4 import BeautifulSoup
from pathlib import Path
import requests

URL = "https://www.cnbc.com/world/"

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh: Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)Version/17.4 Safari/605.1.15",      
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q= 0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}
resp = requests.get(URL, headers = headers,  timeout = 20)
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")

out_path = Path(__file__).resolve().parents[1] / "data" / "raw_data" / "web_data.html"
out_path.parent.mkdir(parents = True, exist_ok = True)
out_path.write_text(soup.prettify(), encoding = "utf-8")
print(f"[OK] Saved: {out_path}")



