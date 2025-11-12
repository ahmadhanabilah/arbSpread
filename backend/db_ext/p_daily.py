# p_daily.py
from __future__ import annotations
from dotenv import load_dotenv
import os, csv, glob, logging
from datetime import datetime
from decimal import Decimal, getcontext
from collections import defaultdict

# ---- Config / Paths
load_dotenv('/root/arbSpread/backend/.env')
logger = logging.getLogger("p_daily")
logger.setLevel(logging.INFO)

getcontext().prec = 50

# Scan both legs by default
FIFO_DIRS = ['/root/arbSpread/backend/db_ext/fifo']
OUT_PATH  = '/root/arbSpread/backend/db_ext/fifo/_daily.csv'   # change if you prefer another location
OUT_FIELDS = ["Date", "PNL", "Volume"]  # PNL from realized_pnl, Volume = sum(|qty| * price), JKT date

# ---- Helpers
def to_dec(x) -> Decimal:
    if x is None:
        return Decimal("0")
    s = str(x).replace(",", "").strip()
    return Decimal(s if s else "0")

def parse_dt_jkt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _iter_fifo_rows(fifo_dirs: list[str]):
    """
    Yield rows from all csv files in the given fifo dirs (skipping files starting with '_').
    """
    for d in fifo_dirs:
        if not os.path.isdir(d):
            continue
        files = [
            p for p in sorted(glob.glob(os.path.join(d, "*.csv")))
            if not os.path.basename(p).startswith("_")
        ]
        for path in files:
            try:
                with open(path, newline="", encoding="utf-8") as f:
                    r = csv.DictReader(f)
                    for row in r:
                        yield row
            except Exception as e:
                logger.info(f"❌ Skipping {path}: {e}")

def build_daily(
    fifo_dirs: list[str] = FIFO_DIRS,
    out_path: str = OUT_PATH,
):
    """
    Build a daily aggregation:
      - Date   : JKT date from `readable_time`
      - PNL    : sum of realized_pnl
      - Volume : sum of |qty| * price
    """
    daily = defaultdict(lambda: {"pnl": Decimal("0"), "vol": Decimal("0")})
    rows_seen = 0

    for row in _iter_fifo_rows(fifo_dirs):
        ts = row.get("readable_time")
        dt = parse_dt_jkt(ts)
        if not dt:
            continue

        key_date = dt.date().isoformat()

        qty   = to_dec(row.get("qty"))
        price = to_dec(row.get("price"))
        # realized_pnl column name is exactly `realized_pnl` in your new schema
        realized = to_dec(row.get("realized_pnl"))

        daily[key_date]["pnl"] += realized
        daily[key_date]["vol"] += qty.copy_abs() * price
        rows_seen += 1

    if rows_seen == 0:
        logger.info("⚠️ No FIFO rows found to aggregate.")
        return

    out_rows = [
        {"Date": d, "PNL": str(v["pnl"]), "Volume": str(v["vol"])}
        for d, v in daily.items()
    ]
    # Sort by Date descending for dashboard convenience
    out_rows.sort(key=lambda x: x["Date"], reverse=True)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    logger.info(f"✅ Daily written to {out_path} (from {rows_seen} rows)")

if __name__ == "__main__":
    build_daily()
