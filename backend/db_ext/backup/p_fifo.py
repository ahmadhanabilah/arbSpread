import logging

logger                          = logging.getLogger("db_ext.p_fifo")
logger.setLevel                 (logging.INFO)

def process_all_fifo():
    in_dir      = '/root/arbSpread/backend/db_ext/raw'
    out_dir     = '/root/arbSpread/backend/db_ext/fifo'

    import os, glob, csv
    from decimal import Decimal, getcontext
    from datetime import datetime, timedelta
    from collections import defaultdict

    getcontext().prec = 50

    ALIASES = {
        "market": ["market", "symbol", "market_name"],
        "time": ["created_time", "timestamp", "time"],
        "fee": ["fee", "fees"],
        "price": ["price", "avg_price", "fill_price"],
        "qty": ["qty", "quantity", "size"],
        "side": ["side", "taker_side", "direction"],
    }

    OUTPUT_FIELDS = [
        "market", "readable_time", "qty", "price", "fees",
        "trade_type", "pnl", "net_pnl", "running_qty",
        "avg_entry_price", "avg_exit_price", "flip"  # <-- added
    ]

    # --- Helpers ---
    def detect_columns(headers):
        headers_lower = {h.lower(): h for h in headers}
        result = {}
        for key, candidates in ALIASES.items():
            for c in candidates:
                if c.lower() in headers_lower:
                    result[key] = headers_lower[c.lower()]
                    break
        must = ["market", "time", "price", "qty"]
        missing = [m for m in must if m not in result]
        if missing:
            raise RuntimeError(f"Missing required columns: {missing}")
        return result

    def to_dec(x):
        if x is None:
            return Decimal("0")
        s = str(x).replace(",", "").strip()
        if s == "":
            return Decimal("0")
        return Decimal(s)

    def normalize_ts(v):
        v = str(v).strip()
        if v == "":
            return 0
        try:
            val = int(v)
        except Exception:
            val = int(float(v))
        if val > 10**12:
            val //= 1000
        return val

    def signed_qty(q_raw, side_raw):
        q = to_dec(q_raw)
        if side_raw is None:
            return q
        s = str(side_raw).strip().upper()
        if s in ("BUY", "LONG", "BID"):
            return q.copy_abs()
        if s in ("SELL", "SHORT", "ASK"):
            return -q.copy_abs()
        return q

    def to_readable_jkt(ts):
        dt = datetime.utcfromtimestamp(int(ts)) + timedelta(hours=7)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def compute_pnl(close_qty, exit_price, was_long, avg_entry):
        if was_long:
            return close_qty * (exit_price - avg_entry)
        else:
            return close_qty * (avg_entry - exit_price)

    def process_market_rows(rows, colmap):
        rows_sorted = sorted(rows, key=lambda r: normalize_ts(r[colmap["time"]]))
        out = []

        running_qty = Decimal("0")
        avg_entry = Decimal("0")
        exit_qty_acc = Decimal("0")
        avg_exit = Decimal("0")

        for r in rows_sorted:
            ts = normalize_ts(r[colmap["time"]])
            price = to_dec(r[colmap["price"]])
            fee = to_dec(r[colmap["fee"]]) if "fee" in colmap else Decimal("0")
            s_qty = signed_qty(r[colmap["qty"]], r.get(colmap.get("side", ""), None))

            def emit(qty, ttype, pnl, rq_after, ae, ax, flip_flag="is_not_flip"):
                out.append({
                    "market": r[colmap["market"]],
                    "readable_time": to_readable_jkt(ts),
                    "qty": str(qty),
                    "price": str(price),
                    "fees": str(fee),
                    "trade_type": ttype,
                    "pnl": str(pnl),
                    "net_pnl": str(pnl - fee),
                    "running_qty": str(rq_after),
                    "avg_entry_price": str(ae),
                    "avg_exit_price": str(ax),
                    "flip": flip_flag,
                })

            if s_qty == 0:
                ttype = "ADD_L" if running_qty >= 0 else "ADD_S"
                emit(Decimal("0"), ttype, Decimal("0"), running_qty, avg_entry, avg_exit)
                continue

            if running_qty == 0:
                avg_entry = price
                running_qty = s_qty
                ttype = "ADD_L" if s_qty > 0 else "ADD_S"
                emit(s_qty, ttype, Decimal("0"), running_qty, avg_entry, Decimal("0"))
                continue

            same_side = (running_qty > 0 and s_qty > 0) or (running_qty < 0 and s_qty < 0)

            if same_side:
                new_abs = running_qty.copy_abs() + s_qty.copy_abs()
                if new_abs != 0:
                    avg_entry = (running_qty.copy_abs() * avg_entry + s_qty.copy_abs() * price) / new_abs
                running_qty += s_qty
                ttype = "ADD_L" if running_qty > 0 else "ADD_S"
                emit(s_qty, ttype, Decimal("0"), running_qty, avg_entry, avg_exit)

            else:
                was_long = running_qty > 0
                if s_qty.copy_abs() < running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty
                    exit_qty_acc = new_exit_qty
                    running_qty += s_qty
                    ttype = "REDUCE_L" if was_long else "REDUCE_S"
                    emit(s_qty, ttype, pnl, running_qty, avg_entry, avg_exit)

                elif s_qty.copy_abs() == running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    pnl = compute_pnl(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty
                    exit_qty_acc = new_exit_qty
                    ttype = "CLOSE_L" if was_long else "CLOSE_S"
                    emit(s_qty, ttype, pnl, Decimal("0"), avg_entry, avg_exit)
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")

                else:
                    # --- FLIP: mark both rows as is_flip ---
                    close_qty = running_qty.copy_abs()
                    total_abs = s_qty.copy_abs()
                    fee_close = fee * (close_qty / total_abs) if total_abs != 0 else Decimal("0")
                    fee_add = fee - fee_close
                    pnl_close = compute_pnl(close_qty, price, was_long, avg_entry)
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / (exit_qty_acc + close_qty)

                    # CLOSE leg to flat
                    out.append({
                        "market": r[colmap["market"]],
                        "readable_time": to_readable_jkt(ts),
                        "qty": str(-running_qty),
                        "price": str(price),
                        "fees": str(fee_close),
                        "trade_type": "CLOSE_L" if was_long else "CLOSE_S",
                        "pnl": str(pnl_close),
                        "net_pnl": str(pnl_close - fee_close),
                        "running_qty": "0",
                        "avg_entry_price": str(avg_entry),
                        "avg_exit_price": str(avg_exit),
                        "flip": "is_flip",  # <-- mark flip
                    })

                    # reset cycle
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")

                    # ADD leg for leftover in new direction
                    leftover = total_abs - close_qty
                    leftover_signed = leftover if s_qty > 0 else -leftover
                    running_qty = leftover_signed
                    avg_entry = price
                    ttype_add = "ADD_L" if leftover_signed > 0 else "ADD_S"
                    out.append({
                        "market": r[colmap["market"]],
                        "readable_time": to_readable_jkt(ts),
                        "qty": str(leftover_signed),
                        "price": str(price),
                        "fees": str(fee_add),
                        "trade_type": ttype_add,
                        "pnl": "0",
                        "net_pnl": str(Decimal("0") - fee_add),
                        "running_qty": str(running_qty),
                        "avg_entry_price": str(avg_entry),
                        "avg_exit_price": "0",
                        "flip": "is_flip",  # <-- mark flip
                    })
        return out

    # --- Main Loop ---
    os.makedirs(out_dir, exist_ok=True)
    files = [
        f for f in sorted(glob.glob(os.path.join(in_dir, "*.csv")))
        if not os.path.basename(f).startswith("_")
    ]
    if not files:
        logger.info(f"⚠️ No CSV files found in {in_dir}")
        return

    for src in files:
        try:
            name = os.path.basename(src)
            dst = os.path.join(out_dir, name)
            with open(src, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                colmap = detect_columns(headers)
                rows = list(reader)

            by_market = defaultdict(list)
            for r in rows:
                by_market[r[colmap["market"]]].append(r)

            all_out = []
            for mrows in by_market.values():
                all_out.extend(process_market_rows(mrows, colmap))

            with open(dst, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
                writer.writeheader()
                for row in all_out:
                    writer.writerow(row)
        except Exception as e:
            logger.info(f"❌ Failed {src}: {e}")


import os, glob, csv
from datetime import datetime

def build_allSymbols(
    fifo_dir="/root/arbSpread/backend/db_ext/fifo",
    out_path="/root/arbSpread/backend/db_ext/fifo/_allSymbols.csv",
):
    """
    Merge all per-symbol FIFO CSVs in fifo_dir into a single _allSymbols.csv.
    - Skips files starting with "_"
    - Sorts by readable_time ASC, then market
    - Preserves columns (including 'flip')
    """
    OUTPUT_FIELDS = [
        "market", "readable_time", "qty", "price", "fees",
        "trade_type", "pnl", "net_pnl", "running_qty",
        "avg_entry_price", "avg_exit_price", "flip"
    ]

    # Ensure output directory exists
    os.makedirs(fifo_dir, exist_ok=True)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    files = [
        f for f in sorted(glob.glob(os.path.join(fifo_dir, "*.csv")))
        if not os.path.basename(f).startswith("_")
    ]
    if not files:
        logger.info(f"⚠️ No FIFO CSVs found in {fifo_dir}")
        return

    rows = []
    for path in files:
        try:
            with open(path, newline="", encoding="utf-8") as f:
                r = csv.DictReader(f)
                for row in r:
                    if not row.get("market") or not row.get("readable_time"):
                        continue
                    rows.append(row)
        except Exception as e:
            logger.info(f"❌ Skipping {path}: {e}")

    if not rows:
        logger.info("⚠️ No data rows found to merge.")
        return

    # sort by readable_time ASC, then market
    def _parse_dt(s):
        try:
            return datetime.strptime((s or "").strip(), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    rows.sort(key=lambda r: (_parse_dt(r.get("readable_time")) or datetime.min, r.get("market","")), reverse=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        w.writeheader()
        for row in rows:
            safe = {k: row.get(k, "") for k in OUTPUT_FIELDS}
            w.writerow(safe)

    logger.info(f"📦 Merged → {out_path}")


import csv, os
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
from collections import defaultdict

getcontext().prec = 50

def build_daily_pnl(
    all_symbols_path="/root/arbSpread/backend/db_ext/fifo/_allSymbols.csv",
    out_path        ="/root/arbSpread/backend/db_ext/fifo/_sum.csv",
    jkt_utc_offset_hours=7
):
    """
    Reads FIFO _allSymbols.csv and writes a daily aggregation CSV (UTC-based):
      readable_date, volume, net_pnl

    volume = sum(|qty| * price)  # turnover-style; change to signed by removing abs() below.
    net_pnl = sum(net_pnl)
    """

    def to_dec(x):
        if x is None:
            return Decimal("0")
        s = str(x).replace(",", "").strip()
        return Decimal(s if s != "" else "0")

    def parse_jkt(ts_str):
        # input readable_time is JKT in your pipeline: "YYYY-MM-DD HH:MM:SS"
        try:
            return datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    if not os.path.exists(all_symbols_path):
        logger.info(f"⚠️ File not found: {all_symbols_path}")
        return

    # Aggregate per UTC date
    # use_abs_qty: set to False if you truly want signed qty*price
    use_abs_qty = True
    daily = defaultdict(lambda: {"volume": Decimal("0"), "net": Decimal("0")})

    with open(all_symbols_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ts_str = row.get("readable_time", "")
            if not ts_str:
                continue
            dt_jkt = parse_jkt(ts_str)
            if dt_jkt is None:
                continue
            # convert to UTC date
            dt_utc = dt_jkt - timedelta(hours=jkt_utc_offset_hours)
            key_date = dt_utc.date().isoformat()  # "YYYY-MM-DD"

            qty = to_dec(row.get("qty"))
            price = to_dec(row.get("price"))
            net_pnl = to_dec(row.get("net_pnl"))

            # volume = sum(|qty| * price) (turnover)
            vol = (qty.copy_abs() if use_abs_qty else qty) * price

            daily[key_date]["volume"] += vol
            daily[key_date]["net"] += net_pnl

    # Ensure output directory
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Write CSV sorted by date ascending
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["readable_date", "volume", "net_pnl"])
        w.writeheader()
        for d in sorted(daily.keys(), reverse=True):
            w.writerow({
                "readable_date": d,
                "volume": str(daily[d]["volume"]),
                "net_pnl": str(daily[d]["net"]),
            })

    logger.info(f"✅ Daily PnL written to {out_path}")
