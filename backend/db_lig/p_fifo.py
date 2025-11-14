from dotenv import load_dotenv
import os, csv, glob, json, logging
from datetime import datetime, timedelta
from decimal import Decimal, getcontext

load_dotenv('/root/arbSpread/backend/.env')

logger = logging.getLogger("db_lig.p_fifo")
logger.setLevel(logging.INFO)

getcontext().prec = 50

# ----- Constants
RAW_DIR   = '/root/arbSpread/backend/db_lig/raw'
FIFO_DIR  = '/root/arbSpread/backend/db_lig/fifo'
FF_PATH   = '/root/arbSpread/backend/db_lig/raw/_fundings.csv'

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

# ----- Small helpers
def to_dec(x) -> Decimal:
    if x is None:
        return Decimal("0")
    s = str(x).replace(",", "").strip()
    return Decimal(s if s else "0")

def parse_epochish(v: str) -> int:
    s = str(v or "").strip()
    if not s:
        return 0
    try:
        val = int(s)
    except Exception:
        val = int(float(s))
    return val // 1000 if val > 10**12 else val

def readable_jkt_from_epoch(sec: int) -> str:
    dt = datetime.utcfromtimestamp(int(sec)) + timedelta(hours=7)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def parse_jkt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def epoch_from_readable(row) -> int:
    dt = parse_jkt(row.get("readable_time"))
    return int(dt.timestamp()) if dt else 0

def abs_qty(row) -> float:
    try:
        return abs(float(str(row.get("qty", "0")).replace(",", "").strip() or 0))
    except Exception:
        return 0.0

def row_sort_key(row):
    return (
        -epoch_from_readable(row),                                     # readable_time DESC
        TYPE_PRIORITY.get((row.get("trade_type") or "").strip(), 9),   # type priority
        -abs_qty(row),                                                 # bigger first (tiebreaker)
    )

def ensure_headers_and_write(path: str, rows: list[dict], headers: list[str]):
    headers = list(dict.fromkeys(headers))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            # cast Decimals to str, drop helper keys
            rr = {k: (str(v) if isinstance(v, Decimal) else v)
                  for k, v in r.items()
                  if not k.endswith("_dt")}
            # keep only declared headers
            rr = {k: rr.get(k, "") for k in headers}
            w.writerow(rr)

# ----- Schema helpers
def detect_generic_columns(headers):
    hl = {h.lower(): h for h in headers}
    pick = lambda *names: next((hl[n] for n in names if n in hl), None)
    col_market = pick("market","symbol","market_name","pair")
    col_time   = pick("created_time","timestamp","time")
    col_price  = pick("price","avg_price","fill_price")
    col_qty    = pick("qty","quantity","size","amount")
    col_side   = pick("side","taker_side","direction")
    col_fee    = pick("fee","fees","maker_fee","taker_fee")
    if not (col_time and col_price and col_qty):
        return None
    return {"market": col_market, "time": col_time, "price": col_price,
            "qty": col_qty, "side": col_side, "fee": col_fee}

def is_apex_schema(headers):
    hl = {h.lower() for h in headers}
    need = {
        "ask_account_id","bid_account_id","is_maker_ask",
        "maker_fee","taker_fee","usd_amount","timestamp","price","size"
    }
    return need.issubset(hl)

# ----- PnL helper
def compute_pnl(close_qty: Decimal, exit_price: Decimal, was_long: bool, avg_entry: Decimal) -> Decimal:
    return close_qty * (exit_price - avg_entry) if was_long else close_qty * (avg_entry - exit_price)

# ----- FIFO engines
def fifo_process_apex(rows, headers, my_account_id: str, default_market: str) -> list[dict]:
    hl = {h.lower(): h for h in headers}
    col_time   = hl["timestamp"]
    col_price  = hl["price"]
    col_size   = hl["size"]
    col_mkrfee = hl["maker_fee"]
    col_tkrfee = hl["taker_fee"]
    col_askacc = hl["ask_account_id"]
    col_bidacc = hl["bid_account_id"]
    col_mkrask = hl["is_maker_ask"]
    col_usd    = hl["usd_amount"]

    my_rows = [r for r in rows if str(r[col_askacc]).strip()==my_account_id or str(r[col_bidacc]).strip()==my_account_id]
    my_rows.sort(key=lambda r: parse_epochish(r[col_time]))

    out = []
    running_qty = Decimal("0")
    avg_entry = Decimal("0")
    exit_qty_acc = Decimal("0")
    avg_exit = Decimal("0")

    def emit_row(ts, qty, price, ttype, trade_pnl: Decimal, trading_fees: Decimal):
        # funding is assigned later → 0 now
        realized = trade_pnl - trading_fees  # funding added later
        return {
            "market": default_market,
            "readable_time": readable_jkt_from_epoch(ts),
            "qty": str(qty),
            "price": str(price),
            "trade_type": ttype,
            "trade_pnl": str(trade_pnl),
            "realized_pnl": str(realized),
            "trading_fees": str(trading_fees),
            "funding_fees": "0",
            "funding_fee_details": "[]",
        }

    for r in my_rows:
        ts       = parse_epochish(r[col_time])
        price    = to_dec(r[col_price])
        size_abs = to_dec(r[col_size]).copy_abs()
        usd_val  = to_dec(r[col_usd])
        is_maker_ask = str(r[col_mkrask]).strip().lower() == "true"

        ask_id = str(r[col_askacc]).strip()
        if ask_id == my_account_id:            # I am ASK → sell/short
            s_qty = -size_abs
            i_am_maker = is_maker_ask
        else:                                  # I am BID → buy/long
            s_qty = size_abs
            i_am_maker = (not is_maker_ask)

        fee_rate_units = to_dec(r[col_mkrfee] if i_am_maker else r[col_tkrfee])
        fee_amt = usd_val * (fee_rate_units / Decimal("1000000"))

        if s_qty == 0:
            out.append(emit_row(ts, Decimal("0"), price, "ADD_L" if running_qty >= 0 else "ADD_S", Decimal("0"), Decimal("0")))
            continue

        if running_qty == 0:
            avg_entry = price
            running_qty = s_qty
            out.append(emit_row(ts, s_qty, price, "ADD_L" if s_qty > 0 else "ADD_S", Decimal("0"), fee_amt))
            continue

        same_side = (running_qty > 0) == (s_qty > 0)
        if same_side:
            new_abs = running_qty.copy_abs() + s_qty.copy_abs()
            if new_abs != 0:
                avg_entry = (running_qty.copy_abs() * avg_entry + s_qty.copy_abs() * price) / new_abs
            running_qty += s_qty
            out.append(emit_row(ts, s_qty, price, "ADD_L" if running_qty > 0 else "ADD_S", Decimal("0"), fee_amt))
        else:
            was_long = running_qty > 0
            if s_qty.copy_abs() < running_qty.copy_abs():
                close_qty = s_qty.copy_abs()
                trade_pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                new_exit_qty = exit_qty_acc + close_qty
                avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else Decimal("0")
                exit_qty_acc = new_exit_qty
                running_qty += s_qty
                out.append(emit_row(ts, s_qty, price, "REDUCE_L" if was_long else "REDUCE_S", trade_pnl, fee_amt))
            elif s_qty.copy_abs() == running_qty.copy_abs():
                close_qty = s_qty.copy_abs()
                trade_pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                new_exit_qty = exit_qty_acc + close_qty
                avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                exit_qty_acc = new_exit_qty
                out.append(emit_row(ts, s_qty, price, "CLOSE_L" if was_long else "CLOSE_S", trade_pnl, fee_amt))
                running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")
            else:
                # flip: split fee proportionally
                close_qty   = running_qty.copy_abs()
                total_abs   = s_qty.copy_abs()
                fee_close   = fee_amt * (close_qty / total_abs) if total_abs != 0 else Decimal("0")
                fee_add     = fee_amt - fee_close
                trade_pnl_close   = compute_pnl(close_qty, price, was_long, avg_entry)
                new_exit_qty= exit_qty_acc + close_qty
                avg_exit    = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                exit_qty_acc= new_exit_qty

                # CLOSE leg
                out.append(emit_row(ts, -running_qty, price, "CLOSE_L" if was_long else "CLOSE_S", trade_pnl_close, fee_close))

                # reset cycle
                running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")

                # ADD leg
                leftover = total_abs - close_qty
                leftover_signed = leftover if s_qty > 0 else -leftover
                running_qty = leftover_signed
                avg_entry = price
                out.append(emit_row(ts, leftover_signed, price, "ADD_L" if leftover_signed > 0 else "ADD_S", Decimal("0"), fee_add))

    return out

def fifo_process_generic(rows, headers, default_market: str) -> list[dict]:
    cmap = detect_generic_columns(headers)
    if cmap is None:
        raise RuntimeError("Cannot detect compatible columns in generic file.")

    by_mkt: dict[str, list[dict]] = {}
    for r in rows:
        m = r[cmap["market"]] if cmap["market"] else default_market
        by_mkt.setdefault(m, []).append(r)

    out = []
    for mkt, mrows in by_mkt.items():
        mrows.sort(key=lambda r: parse_epochish(r[cmap["time"]]))
        running_qty = Decimal("0")
        avg_entry   = Decimal("0")
        exit_qty_acc= Decimal("0")
        avg_exit    = Decimal("0")

        def emit_row(ts, qty, price, ttype, trade_pnl: Decimal, trading_fees: Decimal):
            realized = trade_pnl - trading_fees
            return {
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
            }

        for r in mrows:
            ts    = parse_epochish(r[cmap["time"]])
            price = to_dec(r[cmap["price"]])
            fee   = to_dec(r[cmap["fee"]]) if cmap["fee"] else Decimal("0")

            # signed qty
            if cmap["side"]:
                side = str(r.get(cmap["side"], "")).strip().upper()
                qabs = to_dec(r[cmap["qty"]]).copy_abs()
                if side in ("BUY","LONG","BID"): s_qty = qabs
                elif side in ("SELL","SHORT","ASK"): s_qty = -qabs
                else: s_qty = to_dec(r[cmap["qty"]])
            else:
                s_qty = to_dec(r[cmap["qty"]])

            if s_qty == 0:
                out.append(emit_row(ts, Decimal("0"), price, "ADD_L" if running_qty >= 0 else "ADD_S", Decimal("0"), Decimal("0")))
                continue

            if running_qty == 0:
                avg_entry = price
                running_qty = s_qty
                out.append(emit_row(ts, s_qty, price, "ADD_L" if s_qty > 0 else "ADD_S", Decimal("0"), fee))
                continue

            same_side = (running_qty > 0) == (s_qty > 0)
            if same_side:
                new_abs = running_qty.copy_abs() + s_qty.copy_abs()
                if new_abs != 0:
                    avg_entry = (running_qty.copy_abs() * avg_entry + s_qty.copy_abs() * price) / new_abs
                running_qty += s_qty
                out.append(emit_row(ts, s_qty, price, "ADD_L" if running_qty > 0 else "ADD_S", Decimal("0"), fee))
            else:
                was_long = running_qty > 0
                if s_qty.copy_abs() < running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    trade_pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else Decimal("0")
                    exit_qty_acc = new_exit_qty
                    running_qty += s_qty
                    out.append(emit_row(ts, s_qty, price, "REDUCE_L" if was_long else "REDUCE_S", trade_pnl, fee))
                elif s_qty.copy_abs() == running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    trade_pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                    exit_qty_acc = new_exit_qty
                    out.append(emit_row(ts, s_qty, price, "CLOSE_L" if was_long else "CLOSE_S", trade_pnl, fee))
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")
                else:
                    close_qty = running_qty.copy_abs()
                    total_abs = s_qty.copy_abs()
                    fee_close = fee * (close_qty / total_abs) if total_abs != 0 else Decimal("0")
                    fee_add   = fee - fee_close
                    trade_pnl_close = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                    exit_qty_acc = new_exit_qty

                    # CLOSE leg
                    out.append(emit_row(ts, -running_qty, price, "CLOSE_L" if was_long else "CLOSE_S", trade_pnl_close, fee_close))

                    # reset cycle
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")

                    # ADD leg
                    leftover = total_abs - close_qty
                    leftover_signed = leftover if s_qty > 0 else -leftover
                    running_qty = leftover_signed
                    avg_entry = price
                    out.append(emit_row(ts, leftover_signed, price, "ADD_L" if leftover_signed > 0 else "ADD_S", Decimal("0"), fee_add))
    return out

# ----- Funding integrator
def integrate_funding_into_trades(trades_path: str, fundings_path: str):
    if not (os.path.exists(trades_path) and os.path.exists(fundings_path)):
        return

    # Load trades
    with open(trades_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        trades = list(reader)
        fieldnames = reader.fieldnames or []

    # Guarantee funding columns
    for col in ("funding_fees", "funding_fee_details", "trading_fees", "trade_pnl", "realized_pnl"):
        if col not in fieldnames:
            fieldnames.append(col)

    # Prepare trades
    for t in trades:
        t["_dt"] = parse_jkt(t.get("readable_time"))
        # ensure numeric strings
        t["trade_pnl"]    = str(to_dec(t.get("trade_pnl")))
        t["trading_fees"] = str(to_dec(t.get("trading_fees")))
        # initialize funding fields if missing
        ff_details = t.get("funding_fee_details")
        if not ff_details:
            t["funding_fee_details"] = "[]"
        else:
            try:
                json.loads(ff_details if isinstance(ff_details, str) else "[]")
            except Exception:
                t["funding_fee_details"] = "[]"
        if not t.get("funding_fees"):
            t["funding_fees"] = "0"

        # recompute realized (in case not present)
        trade_pnl = to_dec(t["trade_pnl"])
        trade_fee = to_dec(t["trading_fees"])
        fund_fee  = to_dec(t["funding_fees"])
        t["realized_pnl"] = str(trade_pnl - trade_fee + fund_fee)

    # Load fundings (UTC→JKT)
    with open(fundings_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        all_fundings = []
        for row in reader:
            sym = (row.get("symbol") or row.get("market") or "").strip()
            if not sym or not row.get("change"):
                continue
            ts_raw = row.get("timestamp")
            ts = parse_epochish(ts_raw)
            ts_dt = datetime.utcfromtimestamp(ts) + timedelta(hours=7) if ts else None
            all_fundings.append({"symbol": sym, "ts": ts_dt, "change": to_dec(row.get("change"))})

    symbol = os.path.splitext(os.path.basename(trades_path))[0]
    fundings = [f for f in all_fundings if f["symbol"] == symbol]
    if fundings:
        fundings.sort(key=lambda f: f["ts"] or datetime.min)
        trades.sort(key=lambda t: t["_dt"] or datetime.min)

        pending_sum: Decimal = Decimal("0")
        pending_list: list[float] = []

        for fitem in fundings:
            ts = fitem["ts"]
            amt = fitem["change"]
            if ts is None:
                continue

            matched = False
            for t in trades:
                if t["_dt"] and t["_dt"] >= ts and (t.get("trade_type") or "").startswith(("CLOSE", "REDUCE")):
                    # attach here
                    try:
                        details = json.loads(t["funding_fee_details"]) if isinstance(t["funding_fee_details"], str) else []
                    except Exception:
                        details = []
                    details.extend(pending_list + [float(amt)])

                    # update funding fee & realized
                    new_funding = to_dec(t["funding_fees"]) + pending_sum + amt
                    t["funding_fees"] = str(new_funding)
                    t["funding_fee_details"] = json.dumps(details)

                    trade_pnl = to_dec(t["trade_pnl"])
                    trade_fee = to_dec(t["trading_fees"])
                    t["realized_pnl"] = str(trade_pnl - trade_fee + new_funding)

                    pending_sum = Decimal("0")
                    pending_list = []
                    matched = True
                    break

            if not matched:
                pending_sum += amt
                pending_list.append(float(amt))

        if pending_sum != 0:
            logger.info(f"⚠️ {symbol}: {pending_sum} funding left unassigned")

    # Final write-back
    for t in trades:
        t.pop("_dt", None)
    ensure_headers_and_write(trades_path, trades, fieldnames)

# ----- Main processors
def process_all_fifo():
    logger.info('process_all_fifo started')
    my_account_id = (os.getenv("LIGHTER_ACCOUNT_INDEX") or "").strip()

    os.makedirs(FIFO_DIR, exist_ok=True)
    files = [f for f in sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
             if not os.path.basename(f).startswith("_")]
    if not files:
        logger.info(f"⚠️ No CSV files found in {RAW_DIR}")
        return

    for src in files:
        name = os.path.basename(src)
        stem = os.path.splitext(name)[0]
        default_market = stem if "-" in stem else f"{stem}-USD"
        dst = os.path.join(FIFO_DIR, name)

        try:
            with open(src, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                rows = list(reader)

            if is_apex_schema(headers):
                out_rows = fifo_process_apex(rows, headers, my_account_id, default_market)
            else:
                out_rows = fifo_process_generic(rows, headers, default_market)

            out_rows.sort(key=row_sort_key)
            ensure_headers_and_write(dst, out_rows, OUTPUT_FIELDS)

            # integrate funding & final sort
            integrate_funding_into_trades(dst, FF_PATH)
            with open(dst, newline="", encoding="utf-8") as f:
                rr = csv.DictReader(f)
                out2 = list(rr)
                hdrs = rr.fieldnames or OUTPUT_FIELDS
            out2.sort(key=row_sort_key)
            ensure_headers_and_write(dst, out2, hdrs)

        except Exception as e:
            logger.info(f"❌ Failed {src}: {e}")

# ----- Merge & Daily PnL (optional)
def build_allSymbols(
    fifo_dir=FIFO_DIR,
    out_path=os.path.join(FIFO_DIR, "_allSymbols.csv"),
):
    os.makedirs(fifo_dir, exist_ok=True)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    files = [f for f in sorted(glob.glob(os.path.join(fifo_dir, "*.csv")))
             if not os.path.basename(f).startswith("_")]
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
        logger.info("⚠️ No data rows found to merge.")
        return

    rows.sort(key=lambda r: ( -epoch_from_readable(r), r.get("market","") ))
    ensure_headers_and_write(out_path, rows, OUTPUT_FIELDS)
    logger.info(f"📦 Merged → {out_path}")























