# p_cycle_ext.py
from __future__ import annotations
from dotenv import load_dotenv
import os, csv, glob, json, logging
from datetime import datetime
from decimal import Decimal, getcontext

# ---- Configs / Paths
load_dotenv('/root/arbSpread/backend/.env')
logger = logging.getLogger("db_ext.p_cycle")
logger.setLevel(logging.INFO)

getcontext().prec = 50

FIFO_DIR   = '/root/arbSpread/backend/db_ext/fifo'
CYCLE_DIR  = '/root/arbSpread/backend/db_ext/cycle'
# Note: funding already integrated at FIFO-row level; we aggregate those here.

OUT_FIELDS = [
    "market", "entry_time", "exit_time",
    "qty_opened", "qty_closed", "side",
    "entry_price", "exit_price",
    "trade_pnl", "realized_pnl", "trading_fees",
    "funding_fees", "funding_fee_details",
]

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

def merge_details(a: list[float] | str | None, b: list[float] | str | None) -> list[float]:
    def _norm(v):
        if not v:
            return []
        if isinstance(v, list):
            return [float(x) for x in v]
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return [float(x) for x in parsed]
        except Exception:
            pass
        return []
    return _norm(a) + _norm(b)

def cycle_sort_key(row: dict):
    # Sort by exit_time DESC (empty first), then entry_time DESC
    def _ts(s):
        dt = parse_dt_jkt(s)
        return int(dt.timestamp()) if dt else (10**15 if not s else 0)
    return (-_ts(row.get("exit_time") or ""), -_ts(row.get("entry_time") or ""))

# ---- Core
def build_cycles_for_file(fifo_path: str, out_path: str):
    """
    Collapse FIFO rows (per symbol) into cycle rows:
      - A cycle starts at first ADD_* when running position goes 0 -> nonzero
      - A cycle ends when running position returns to 0 (CLOSE_* or close leg of a flip)
      - Flip is already split in FIFO as CLOSE_* then ADD_*; one row ends the cycle, next row starts new cycle.
      - entry_price = VWAP of ADD legs; exit_price = VWAP of REDUCE/CLOSE legs.
    """
    with open(fifo_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)

    # Reconstruct state in ascending time
    def _row_ts(row):
        dt = parse_dt_jkt(row.get("readable_time"))
        return int(dt.timestamp()) if dt else 0
    rows.sort(key=_row_ts)

    cycles = []

    running_qty = Decimal("0")
    current = _new_empty_cycle()

    # Price accumulators for VWAPs
    entry_notional = Decimal("0")
    entry_qty = Decimal("0")
    exit_notional = Decimal("0")
    exit_qty = Decimal("0")

    for row in rows:
        market = row.get("market") or ""
        ts_str = row.get("readable_time") or ""
        dt = parse_dt_jkt(ts_str)
        qty = to_dec(row.get("qty"))
        price = to_dec(row.get("price"))
        ttype = (row.get("trade_type") or "").strip().upper()

        trade_pnl     = to_dec(row.get("trade_pnl"))
        trading_fees  = to_dec(row.get("trading_fees"))
        funding_fees  = to_dec(row.get("funding_fees"))
        ff_details    = row.get("funding_fee_details") or "[]"

        # Start a new cycle when flat and an ADD_* arrives
        if running_qty == 0 and ttype.startswith("ADD"):
            current = _new_empty_cycle()
            current["market"] = market
            current["entry_time"] = ts_str if dt else ""
            current["side"] = ("long" if qty > 0 else "short")

            entry_notional = Decimal("0")
            entry_qty = Decimal("0")
            exit_notional = Decimal("0")
            exit_qty = Decimal("0")

        # Aggregate
        if ttype.startswith("ADD"):
            current["qty_opened"] += abs(qty)
            current["trading_fees"] += trading_fees
            current["trade_pnl"] += trade_pnl
            current["funding_fees"] += funding_fees
            current["funding_fee_details"] = merge_details(current["funding_fee_details"], ff_details)
            running_qty += qty

            # VWAP entry accumulators
            entry_notional += abs(qty) * price
            entry_qty += abs(qty)

        elif ttype.startswith("REDUCE") or ttype.startswith("CLOSE"):
            current["qty_closed"] += abs(qty)
            current["trading_fees"] += trading_fees
            current["trade_pnl"] += trade_pnl
            current["funding_fees"] += funding_fees
            current["funding_fee_details"] = merge_details(current["funding_fee_details"], ff_details)
            running_qty += qty  # qty sign brings it toward zero

            # VWAP exit accumulators
            exit_notional += abs(qty) * price
            exit_qty += abs(qty)

            if running_qty == 0:
                current["exit_time"] = ts_str if dt else ""
                # Prices
                current["entry_price"] = (entry_notional / entry_qty) if entry_qty != 0 else Decimal("0")
                current["exit_price"]  = (exit_notional / exit_qty)   if exit_qty  != 0 else Decimal("0")
                # Realized
                realized = current["trade_pnl"] - current["trading_fees"] - current["funding_fees"]

                cycles.append(_finalize_cycle_row(current, realized))
                # reset; next ADD_* is a new cycle
                current = _new_empty_cycle()
                entry_notional = entry_qty = exit_notional = exit_qty = Decimal("0")

        else:
            # unknown type: ignore, keep state
            pass

    # Emit partial (still-open) cycle if any
    if current["entry_time"] and (current["qty_opened"] > 0) and (current["market"] or rows):
        # Set prices for partially closed/open cycle
        current["entry_price"] = (entry_notional / entry_qty) if entry_qty != 0 else Decimal("0")
        current["exit_price"]  = (exit_notional / exit_qty)   if exit_qty  != 0 else ""
        realized = current["trade_pnl"] - current["trading_fees"] - current["funding_fees"]
        cycles.append(_finalize_cycle_row(current, realized, open_cycle=True))

    # Sort: open cycles first (empty exit_time), then exit_time desc, then entry_time desc
    cycles.sort(key=cycle_sort_key)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        w.writeheader()
        for c in cycles:
            w.writerow({k: c.get(k, "") for k in OUT_FIELDS})

def _new_empty_cycle():
    return {
        "market": "",
        "entry_time": "",
        "exit_time": "",
        "qty_opened": Decimal("0"),
        "qty_closed": Decimal("0"),
        "side": "",  # "long"/"short"
        "entry_price": Decimal("0"),
        "exit_price": "",  # blank until we have any close
        "trade_pnl": Decimal("0"),
        "trading_fees": Decimal("0"),
        "funding_fees": Decimal("0"),
        "funding_fee_details": [],  # list of floats
    }

def _finalize_cycle_row(current: dict, realized: Decimal, open_cycle: bool=False) -> dict:
    return {
        "market": current["market"],
        "entry_time": current["entry_time"],
        "exit_time": "" if open_cycle else current["exit_time"],
        "qty_opened": str(current["qty_opened"]),
        "qty_closed": str(current["qty_closed"]),
        "side": current["side"],
        "entry_price": str(current["entry_price"]),
        "exit_price":  ("" if open_cycle and current["exit_price"] == "" else str(current["exit_price"])),
        "trade_pnl": str(current["trade_pnl"]),
        "realized_pnl": str(realized),
        "trading_fees": str(current["trading_fees"]),
        "funding_fees": str(current["funding_fees"]),
        "funding_fee_details": json.dumps(current["funding_fee_details"]),
    }

def process_all_cycles():
    """
    Read all symbol CSVs from FIFO_DIR (skip files starting with '_'),
    and write cycle CSVs into CYCLE_DIR with the same filenames.
    """
    os.makedirs(CYCLE_DIR, exist_ok=True)
    files = [p for p in sorted(glob.glob(os.path.join(FIFO_DIR, "*.csv")))
             if not os.path.basename(p).startswith("_")]
    if not files:
        logger.info(f"⚠️ No FIFO CSVs found in {FIFO_DIR}")
        return

    for src in files:
        name = os.path.basename(src)
        dst  = os.path.join(CYCLE_DIR, name)
        try:
            build_cycles_for_file(src, dst)
        except Exception as e:
            logger.info(f"❌ Failed {src}: {e}")

# ---- Merge all cycle files into one table
def _write_csv(path: str, rows: list[dict], headers: list[str]):
    headers = list(dict.fromkeys(headers))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in headers})

def build_allSymbols(
    cycle_dir: str = CYCLE_DIR,
    out_path: str = os.path.join(CYCLE_DIR, "_allSymbols.csv"),
):
    """
    Merge all per-symbol cycle CSVs into a single _allSymbols.csv:
      - Skips files starting with '_'
      - Sorts: open cycles first (empty exit_time), then exit_time DESC, then entry_time DESC
    """
    os.makedirs(cycle_dir, exist_ok=True)
    files = [
        p for p in sorted(glob.glob(os.path.join(cycle_dir, "*.csv")))
        if not os.path.basename(p).startswith("_")
    ]
    if not files:
        logger.info(f"⚠️ No cycle CSVs found in {cycle_dir}")
        return

    rows: list[dict] = []
    for path in files:
        try:
            with open(path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    if not row.get("market") or not row.get("entry_time"):
                        continue
                    rows.append(row)
        except Exception as e:
            logger.info(f"❌ Skipping {path}: {e}")

    if not rows:
        logger.info("⚠️ No cycle rows to merge.")
        return

    rows.sort(key=cycle_sort_key)
    _write_csv(out_path, rows, OUT_FIELDS)
    logger.info(f"📦 Cycle merged → {out_path}")

if __name__ == "__main__":
    process_all_cycles()
    build_allSymbols()
