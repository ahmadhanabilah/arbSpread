# p_fifo_ext.py
from dotenv import load_dotenv
import os, csv, glob, json, logging
from datetime import datetime, timedelta
from decimal import Decimal, getcontext

# --- Config
load_dotenv('/root/arbSpread/backend/.env')
logger = logging.getLogger("db_ext.p_fifo")
logger.setLevel(logging.INFO)
getcontext().prec = 50

RAW_DIR   = '/root/arbSpread/backend/db_ext/raw'
FIFO_DIR  = '/root/arbSpread/backend/db_ext/fifo'
FF_PATH   = '/root/arbSpread/backend/db_ext/raw/_fundings.csv'

OUTPUT_FIELDS = [
    "market", "readable_time", "qty", "price", "trade_type",
    "trade_pnl", "realized_pnl", "trading_fees",
    "funding_fees", "funding_fee_details"
]

TYPE_PRIORITY = {
    "CLOSE_L": 0, "CLOSE_S": 0,
    "REDUCE_L": 1, "REDUCE_S": 1,
    "ADD_L":   2, "ADD_S":   2,
}

# --- Small helpers
def to_dec(x) -> Decimal:
    if x is None:
        return Decimal("0")
    s = str(x).replace(",", "").strip()
    return Decimal(s if s else "0")

def parse_epochish(v: str) -> int:
    s = str(v or "").strip()
    if not s: return 0
    try:
        val = int(s)
    except Exception:
        val = int(float(s))
    return val // 1000 if val > 10**12 else val

def readable_jkt_from_epoch(sec: int) -> str:
    dt = datetime.utcfromtimestamp(int(sec)) + timedelta(hours=7)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_jkt(s: str) -> datetime | None:
    if not s: return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def epoch_from_row(r) -> int:
    dt = parse_jkt(r.get("readable_time"))
    return int(dt.timestamp()) if dt else 0

def abs_qty(row) -> float:
    try:
        return abs(float(str(row.get("qty","0")).replace(",","").strip() or 0))
    except Exception:
        return 0.0

def row_sort_key(row):
    return (
        -epoch_from_row(row),
        TYPE_PRIORITY.get((row.get("trade_type") or "").strip(), 9),
        -abs_qty(row),
    )

def ensure_headers_and_write(path: str, rows: list[dict], headers: list[str]):
    headers = list(dict.fromkeys(headers))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            # cast Decimals to str, drop helper keys
            rr = {k: (str(v) if isinstance(v, Decimal) else v)
                  for k, v in r.items()
                  if not k.endswith("_dt")}
            rr = {k: rr.get(k, "") for k in headers}
            w.writerow(rr)

# --- PnL
def compute_pnl(close_qty: Decimal, exit_price: Decimal, was_long: bool, avg_entry: Decimal) -> Decimal:
    return close_qty * (exit_price - avg_entry) if was_long else close_qty * (avg_entry - exit_price)

# --- FIFO for Extended schema
def fifo_process_extended(rows: list[dict]) -> list[dict]:
    """
    Expects rows with these columns (as in db_ext raw):
      - market, created_time (ms/s), price, qty, side (BUY/SELL), fee, is_taker
    We treat 'fee' as the trading fee in quote currency; sign comes from 'side'.
    """
    # group per market
    by_mkt: dict[str, list[dict]] = {}
    for r in rows:
        m = (r.get("market") or "").strip()
        if not m: continue
        by_mkt.setdefault(m, []).append(r)

    out: list[dict] = []

    for mkt, mrows in by_mkt.items():
        # time ASC for FIFO mechanics
        mrows.sort(key=lambda r: parse_epochish(r.get("created_time")))

        running_qty = Decimal("0")
        avg_entry   = Decimal("0")
        exit_qty_acc= Decimal("0")
        avg_exit    = Decimal("0")

        def emit(ts, qty, price, ttype, trade_pnl: Decimal, trading_fees: Decimal):
            realized = trade_pnl - trading_fees  # funding attached later
            out.append({
                "market": mkt,
                "readable_time": readable_jkt_from_epoch(ts),
                "qty": str(qty),
                "price": str(price),
                "trade_type": ttype,
                "trade_pnl": str(trade_pnl),
                "realized_pnl": str(realized),
                "trading_fees": str(trading_fees),
                "funding_fees": "0",
                "funding_fee_details": "[]",
            })

        for r in mrows:
            ts    = parse_epochish(r.get("created_time"))
            price = to_dec(r.get("price"))
            fee   = to_dec(r.get("fee"))  # already absolute fee in quote
            # signed qty from side
            side  = str(r.get("side","")).strip().upper()
            qabs  = to_dec(r.get("qty")).copy_abs()
            if side in ("BUY","LONG","BID"):
                s_qty = qabs
            elif side in ("SELL","SHORT","ASK"):
                s_qty = -qabs
            else:
                # fallback: use sign of qty if present
                s_qty = to_dec(r.get("qty"))

            if s_qty == 0:
                emit(ts, Decimal("0"), price, "ADD_L" if running_qty >= 0 else "ADD_S", Decimal("0"), Decimal("0"))
                continue

            if running_qty == 0:
                avg_entry = price
                running_qty = s_qty
                emit(ts, s_qty, price, "ADD_L" if s_qty > 0 else "ADD_S", Decimal("0"), fee)
                continue

            same_side = (running_qty > 0) == (s_qty > 0)
            if same_side:
                new_abs = running_qty.copy_abs() + s_qty.copy_abs()
                if new_abs != 0:
                    avg_entry = (running_qty.copy_abs() * avg_entry + s_qty.copy_abs() * price) / new_abs
                running_qty += s_qty
                emit(ts, s_qty, price, "ADD_L" if running_qty > 0 else "ADD_S", Decimal("0"), fee)
            else:
                was_long = running_qty > 0
                if s_qty.copy_abs() < running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    trade_pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else Decimal("0")
                    exit_qty_acc = new_exit_qty
                    running_qty += s_qty
                    emit(ts, s_qty, price, "REDUCE_L" if was_long else "REDUCE_S", trade_pnl, fee)
                elif s_qty.copy_abs() == running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    trade_pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                    exit_qty_acc = new_exit_qty
                    emit(ts, s_qty, price, "CLOSE_L" if was_long else "CLOSE_S", trade_pnl, fee)
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")
                else:
                    # flip, split the fee proportionally
                    close_qty = running_qty.copy_abs()
                    total_abs = s_qty.copy_abs()
                    fee_close = fee * (close_qty / total_abs) if total_abs != 0 else Decimal("0")
                    fee_add   = fee - fee_close
                    trade_pnl_close = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                    exit_qty_acc = new_exit_qty

                    # CLOSE leg
                    emit(ts, -running_qty, price, "CLOSE_L" if was_long else "CLOSE_S", trade_pnl_close, fee_close)

                    # reset cycle
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")

                    # ADD leg
                    leftover = total_abs - close_qty
                    leftover_signed = leftover if s_qty > 0 else -leftover
                    running_qty = leftover_signed
                    avg_entry = price
                    emit(ts, leftover_signed, price, "ADD_L" if leftover_signed > 0 else "ADD_S", Decimal("0"), fee_add)

    return out

# --- Funding integration (Extended)
def integrate_funding_into_trades(trades_path: str, fundings_path: str):
    """
    Assign each funding payment to the first CLOSE/REDUCE trade at or after its JKT time.
    Funding CSV columns (examples):
      accountId,fundingFee,fundingRate,id,markPrice,market,paidTime,readable_paidTime,side,size
    We use:
      - market (string match to trades 'market')
      - fundingFee (signed): positive cost, negative rebate
      - readable_paidTime (already JKT)
    """
    if not (os.path.exists(trades_path) and os.path.exists(fundings_path)):# no-op
        return

    # Load trades
    with open(trades_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        trades = list(reader)
        fieldnames = reader.fieldnames or []

    for col in ("funding_fees", "funding_fee_details", "trading_fees", "trade_pnl", "realized_pnl"):
        if col not in fieldnames:
            fieldnames.append(col)

    # Prepare trades
    for t in trades:
        t["_dt"] = parse_jkt(t.get("readable_time"))
        # normalize numeric strings
        t["trade_pnl"]    = str(to_dec(t.get("trade_pnl")))
        t["trading_fees"] = str(to_dec(t.get("trading_fees")))
        if not t.get("funding_fee_details"):
            t["funding_fee_details"] = "[]"
        else:
            try:
                json.loads(t["funding_fee_details"])
            except Exception:
                t["funding_fee_details"] = "[]"
        if not t.get("funding_fees"):
            t["funding_fees"] = "0"
        # recompute realized
        t["realized_pnl"] = str(to_dec(t["trade_pnl"]) - to_dec(t["trading_fees"]) + to_dec(t["funding_fees"]))

    # Load fundings
    with open(fundings_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        all_fundings = []
        for row in r:
            sym = (row.get("market") or row.get("symbol") or "").strip()
            if not sym: continue
            # prefer readable_paidTime (already JKT); fallback to paidTime epoch
            rtime = row.get("readable_paidTime")
            if rtime:
                ts_dt = parse_jkt(rtime)
            else:
                ts = parse_epochish(row.get("paidTime"))
                ts_dt = datetime.utcfromtimestamp(ts) + timedelta(hours=7) if ts else None
            if ts_dt is None: continue
            ff = to_dec(row.get("fundingFee"))
            all_fundings.append({"symbol": sym, "ts": ts_dt, "fee": ff})

    symbol = os.path.splitext(os.path.basename(trades_path))[0]
    fitems = [f for f in all_fundings if f["symbol"] == symbol]
    if fitems:
        fitems.sort(key=lambda x: x["ts"])
        trades.sort(key=lambda t: t["_dt"] or datetime.min)

        pending_sum: Decimal = Decimal("0")
        pending_list: list[float] = []

        for item in fitems:
            ts = item["ts"]; amt = item["fee"]
            matched = False
            for t in trades:
                if t["_dt"] and t["_dt"] >= ts and (t.get("trade_type") or "").startswith(("CLOSE","REDUCE")):
                    # attach here (include any previous unassigned funding)
                    try:
                        details = json.loads(t["funding_fee_details"]) if isinstance(t["funding_fee_details"], str) else []
                    except Exception:
                        details = []
                    details.extend(pending_list + [float(amt)])
                    new_funding = to_dec(t["funding_fees"]) + pending_sum + amt
                    t["funding_fees"] = str(new_funding)
                    t["funding_fee_details"] = json.dumps(details)
                    t["realized_pnl"] = str(to_dec(t["trade_pnl"]) - to_dec(t["trading_fees"]) + new_funding)

                    pending_sum = Decimal("0")
                    pending_list = []
                    matched = True
                    break
            if not matched:
                pending_sum += amt
                pending_list.append(float(amt))

        if pending_sum != 0:
            logger.info(f"⚠️ {symbol}: {pending_sum} funding left unassigned")

    # write back
    for t in trades:
        t.pop("_dt", None)
    ensure_headers_and_write(trades_path, trades, fieldnames)

# --- Main
def process_all_fifo():
    logger.info("process_all_fifo (EXT) started")

    os.makedirs(FIFO_DIR, exist_ok=True)
    files = [p for p in sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
             if not os.path.basename(p).startswith("_")]

    if not files:
        logger.info(f"⚠️ No CSV files found in {RAW_DIR}")
        return

    for src in files:
        name = os.path.basename(src)
        dst  = os.path.join(FIFO_DIR, name)
        try:
            with open(src, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            out_rows = fifo_process_extended(rows)
            out_rows.sort(key=row_sort_key)
            ensure_headers_and_write(dst, out_rows, OUTPUT_FIELDS)

            # integrate funding + final sort
            integrate_funding_into_trades(dst, FF_PATH)
            with open(dst, newline="", encoding="utf-8") as f:
                rr = csv.DictReader(f)
                rows2 = list(rr)
                hdrs = rr.fieldnames or OUTPUT_FIELDS
            rows2.sort(key=row_sort_key)
            ensure_headers_and_write(dst, rows2, hdrs)

        except Exception as e:
            logger.info(f"❌ Failed {src}: {e}")

# --- Optional helpers similar to Lighter
def build_allSymbols(
    fifo_dir=FIFO_DIR,
    out_path=os.path.join(FIFO_DIR, "_allSymbols.csv"),
):
    os.makedirs(fifo_dir, exist_ok=True)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    files = [p for p in sorted(glob.glob(os.path.join(fifo_dir, "*.csv")))
             if not os.path.basename(p).startswith("_")]
    if not files:
        logger.info(f"⚠️ No FIFO CSVs found in {fifo_dir}")
        return

    rows = []
    for path in files:
        try:
            with open(path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    if row.get("market") and row.get("readable_time"):
                        rows.append(row)
        except Exception as e:
            logger.info(f"❌ Skipping {path}: {e}")

    if not rows:
        logger.info("⚠️ No data rows to merge.")
        return

    rows.sort(key=lambda r: (-epoch_from_row(r), r.get("market","")))
    ensure_headers_and_write(out_path, rows, OUTPUT_FIELDS)
    logger.info(f"📦 Merged → {out_path}")
