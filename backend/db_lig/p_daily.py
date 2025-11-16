# p_daily.py
from __future__ import annotations
from dotenv import load_dotenv
import os, csv, glob, logging
from datetime import datetime, timedelta   # ⬅️ already there
from decimal import Decimal, getcontext
from collections import defaultdict

# ---- Config / Paths
load_dotenv('/root/arbSpread/backend/.env')
logger = logging.getLogger("p_daily")
logger.setLevel(logging.INFO)

getcontext().prec = 50

# Scan both legs by default
FIFO_DIRS = ['/root/arbSpread/backend/db_lig/fifo']
OUT_PATH  = '/root/arbSpread/backend/db_lig/fifo/_daily.csv'
FF_PATH   = '/root/arbSpread/backend/db_lig/raw/_fundings.csv'   # ⬅️ NEW

# ⬇️ I only ADD "Funding" column; Date/PNL/Volume tetap ada
OUT_FIELDS = ["Date", "PNL", "Funding", "Volume"]  # Date in UTC by default


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
    out_path: str        = OUT_PATH,
    use_utc: bool        = False,   # ⬅️ default: pakai UTC
    src_utc_offset_hours:int = 7,  # readable_time = UTC+7 (JKT)
):
    """
    Build a daily aggregation:
      - Date   : if use_utc=True  -> UTC date
                 if use_utc=False -> JKT date (as-is)
      - PNL    : sum of trade_pnl + trading_fees (trades only)
      - Funding: sum of funding 'change' from _fundings.csv
      - Volume : sum of |qty| * price
    """
    daily = defaultdict(lambda: {"pnl": Decimal("0"), "vol": Decimal("0"), "ff": Decimal("0")})
    rows_seen = 0

    # ---------- 1) Aggregate trades from FIFO ----------
    for row in _iter_fifo_rows(fifo_dirs):
        ts = row.get("readable_time")
        dt_local = parse_dt_jkt(ts)
        if not dt_local:
            continue

        if use_utc:
            # convert from local (UTC+7) → UTC
            dt = dt_local - timedelta(hours=src_utc_offset_hours)
        else:
            dt = dt_local

        key_date = dt.date().isoformat()

        qty      = to_dec(row.get("qty"))
        price    = to_dec(row.get("price"))
        realized = to_dec(row.get("trade_pnl")) + to_dec(row.get("trading_fees"))

        daily[key_date]["pnl"] += realized
        daily[key_date]["vol"] += qty.copy_abs() * price
        rows_seen += 1

    if rows_seen == 0:
        logger.info("⚠️ No FIFO rows found to aggregate (trades).")

    # ---------- 2) Aggregate funding from _fundings.csv (timestamp already UTC) ----------
    if os.path.exists(FF_PATH):
        try:
            with open(FF_PATH, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                ff_rows = 0
                for row in r:
                    ts_raw = row.get("timestamp")
                    if not ts_raw:
                        continue
                    try:
                        # epoch seconds in UTC
                        ts_int = int(float(ts_raw))
                    except ValueError:
                        continue

                    dt_utc = datetime.utcfromtimestamp(ts_int)

                    if use_utc:
                        dt = dt_utc
                    else:
                        # convert UTC → local (UTC+7)
                        dt = dt_utc + timedelta(hours=src_utc_offset_hours)

                    key_date = dt.date().isoformat()

                    # funding amount from 'change'
                    ff_val = to_dec(row.get("change"))
                    daily[key_date]["ff"] += ff_val
                    ff_rows += 1

            logger.info(f"✅ Aggregated funding from {FF_PATH} ({ff_rows} rows)")
        except Exception as e:
            logger.info(f"❌ Failed reading funding file {FF_PATH}: {e}")
    else:
        logger.info(f"⚠️ Funding file not found: {FF_PATH}")

    if not daily:
        logger.info("⚠️ No data (trades or funding) found to aggregate.")
        return

    # ---------- 3) Build output rows ----------
    out_rows = [
        {
            "Date":    d,
            "PNL":     str(v["pnl"]),
            "Funding": str(v["ff"]),
            "Volume":  str(v["vol"]),
        }
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

    logger.info(f"✅ Daily written to {out_path} (from {rows_seen} FIFO rows + funding), use_utc={use_utc}")


if __name__ == "__main__":
    build_daily()  # default: UTC days from JKT timestamps
