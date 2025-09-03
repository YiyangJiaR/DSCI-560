from pathlib import Path
from bs4 import BeautifulSoup
import csv
import re

Root = Path(__file__).resolve().parents[1]
HTML = Root / "data" / "raw_data" / "web_data.html"
Outdir = Root / "data" / "processed_data"
Outdir.mkdir(parents=True, exist_ok=True)

soup = BeautifulSoup(HTML.read_text(encoding="utf-8", errors="ignore"), "html.parser")

def fitst_percent(text: str) -> str:
    m = re.search(r" [+\-]?\d+(?:\.\d+)?\s*%", text)
    return m.group(0).replace(" ", "") if m else ""

# market_data.csv
market_rows = []
for el in soup.select("[data-symbol]"):
    sym = el.get("data-symbol", "").strip()
    if not sym:
        continue
    pct = first_percent(el.get_text(" ", strip=True))
    if not pct:
        continue
    pos = "Up" if pct.startswith("+") else ("Down" if pct.startswith("-") else "")
    market_rows.append({
        "marketCard_symbol": sym,
        "marketCard_stockPosition": pos,
        "marketCard-changePct": pct
    })

with (Outdir / "market_data.csv").open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["marketCard_symbol", "marketCard_stockPosition", "marketCard-changePct"])
    for r in market_rows:
        w.writerow([r["marketCard_symbol"], r["marketCard_stockPosition"], r["marketCard-changePct"]])

# news_data.csv
news_rows = []
container = soup.select_one('[data-module*="LatestNews" i], .LatestNews, [id*="latest" i]')
items = container.select("li") if container else soup.select("li")

for li in items:
    a = li.find("a", href=True)
    if not a:
        continue
    title = a.get_text(strip=True)
    link = a["href"]
    if not title or not link:
        continue
    t = li.find("time")
    ts = t.get_text(strip=True) if t else ""
    news_rows.append({"LatestNews-timestamp": ts, "title": title, "link": link})

with (Outdir / "news_data.csv").open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["LatestNews-timestamp", "title", "link"])
    for r in news_rows:
        w.writerow([r["LatestNews-timestamp"], r["title"], r["link"]])

print("[OK] Wrote:",
      Outdir / "market_data.csv",
      "and",
      Outdir / "news_data.csv")
