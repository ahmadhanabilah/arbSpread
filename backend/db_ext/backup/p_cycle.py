import csv, os
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
import logging

logger                          = logging.getLogger("db_ext.p_cycle")
logger.setLevel                 (logging.INFO)

getcontext().prec = 50

# --- NEW: funding integration helpers ---

def _parse_dt_jkt(s):
    # "YYYY-MM-DD HH:MM:SS" (already JKT in your pipeline)
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _normalize_epoch_to_sec(x):
    # accepts seconds or milliseconds, returns seconds (int)
    try:
        v = int(str(x).strip())
    except Exception:
        v = int(float(x))
    return v // 1000 if v > 10**12 else v

def load_fundings_csv(funding_path):
    """
    Returns list of dicts:
      { 'market': str, 'ts': datetime(JKT), 'fee': Decimal, 'side': 'LONG'/'SHORT'/..., 'raw_ts': int }
    """
    out = []
    if not os.path.exists(funding_path):
        return out
    with open(funding_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            mk = (row.get("market") or "").strip()
            if mk == "":
                continue
            fee = Decimal(str(row.get("fundingFee", "0")).replace(",", "").strip() or "0")
            # prefer readable_paidTime if present; else paidTime -> JKT
            rs = (row.get("readable_paidTime") or "").strip()
            if rs:
                dt = _parse_dt_jkt(rs)
            else:
                paid = row.get("paidTime")
                if paid is None or str(paid).strip() == "":
                    continue
                sec = _normalize_epoch_to_sec(paid)
                # convert UTC->JKT (+7) to match your trade rows
                dt = datetime.utcfromtimestamp(sec) + timedelta(hours=7)
            out.append({
                "market": mk,
                "ts": dt,
                "fee": fee,
                "side": (row.get("side") or "").upper(),
                "raw_ts": row.get("paidTime")
            })
    return out

def funding_sum_for_window(fundings, market, t_start, t_end):
    """
    Sum funding for a market between [t_start, t_end] inclusive.
    If t_end is None (open cycle), sum up to t_start's day/hour end? We'll sum up to t_start..t_start (no funding),
    but you can change logic to 'up to last funding available' by swapping condition below.
    """
    if t_start is None:
        return Decimal("0")
    s = Decimal("0")
    for r in fundings:
        if r["market"] != market:
            continue
        ts = r["ts"]
        if ts is None:
            continue
        if t_end is not None:
            if t_start <= ts <= t_end:
                s += r["fee"]
        else:
            # open cycle: include funding up to latest trade time we recorded (conservative: none)
            # change to 't_start <= ts' if you want to accrue ongoing funding into open cycles.
            pass
    return s


def build_cycled_csv(input_path, output_path):
    def to_dec(x):
        if x is None:
            return Decimal("0")
        s = str(x).replace(",", "").strip()
        if s == "":
            return Decimal("0")
        return Decimal(s)

    def normalize_ts(val):
        s = str(val).strip()
        if s == "":
            return 0
        try:
            v = int(s)
        except Exception:
            v = int(float(s))
        if v > 10**12:  # ms
            v //= 1000
        return v

    def to_readable_jkt(epoch_seconds):
        dt = datetime.utcfromtimestamp(int(epoch_seconds)) + timedelta(hours=7)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def signed_qty(q_raw, side_raw):
        q = to_dec(q_raw)
        s = str(side_raw).strip().upper() if side_raw is not None else ""
        if s in ("BUY", "LONG", "BID"):
            return q.copy_abs()
        if s in ("SELL", "SHORT", "ASK"):
            return -q.copy_abs()
        return q

    # ---------- load input ----------
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    hdr = [h.lower() for h in headers]
    is_fifo = all(c in hdr for c in [
        "market","readable_time","qty","price","fees","trade_type",
        "pnl","net_pnl","running_qty","avg_entry_price","avg_exit_price"
    ])

    # ---------- RAW -> FIFO (if needed) ----------
    def raw_to_fifo(rows):
        def pick(names):
            for n in names:
                for h in headers:
                    if h.lower() == n:
                        return h
            return None

        col_market = pick(["market","symbol","market_name"])
        col_time   = pick(["created_time","timestamp","time"])
        col_fee    = pick(["fee","fees"])
        col_price  = pick(["price","avg_price","fill_price"])
        col_qty    = pick(["qty","quantity","size"])
        col_side   = pick(["side","taker_side","direction"])

        if not (col_market and col_time and col_price and col_qty):
            raise RuntimeError("RAW->FIFO: missing essential columns")

        # NEW: load fundings

        rows_sorted = sorted(rows, key=lambda r: normalize_ts(r[col_time]))

        out = []
        running_qty = Decimal("0")
        avg_entry = Decimal("0")
        avg_exit = Decimal("0")
        exit_qty_acc = Decimal("0")

        def realized(close_qty, exit_price, was_long, entry):
            if was_long:
                return close_qty * (exit_price - entry)
            else:
                return close_qty * (entry - exit_price)

        for r in rows_sorted:
            ts = normalize_ts(r[col_time])
            price = to_dec(r[col_price])
            fee = to_dec(r[col_fee]) if col_fee else Decimal("0")
            s_qty = signed_qty(r[col_qty], r.get(col_side, None))
            mkt = r[col_market]

            def emit(qty, ttype, pnl, rq_after, ae, ax):
                out.append({
                    "market": mkt,
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
                    pnl = realized(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else Decimal("0")
                    exit_qty_acc = new_exit_qty
                    running_qty += s_qty
                    ttype = "REDUCE_L" if was_long else "REDUCE_S"
                    emit(s_qty, ttype, pnl, running_qty, avg_entry, avg_exit)
                elif s_qty.copy_abs() == running_qty.copy_abs():
                    close_qty = s_qty.copy_abs()
                    pnl = realized(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                    exit_qty_acc = new_exit_qty
                    ttype = "CLOSE_L" if was_long else "CLOSE_S"
                    emit(s_qty, ttype, pnl, Decimal("0"), avg_entry, avg_exit)
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")
                else:
                    close_qty = running_qty.copy_abs()
                    total_abs = s_qty.copy_abs()
                    fee_close = fee * (close_qty / total_abs) if total_abs != 0 else Decimal("0")
                    fee_add = fee - fee_close
                    pnl_close = realized(close_qty, price, was_long, avg_entry)
                    new_exit_qty = exit_qty_acc + close_qty
                    avg_exit = (exit_qty_acc * avg_exit + close_qty * price) / new_exit_qty if new_exit_qty != 0 else price
                    exit_qty_acc = new_exit_qty

                    # close leg
                    out.append({
                        "market": mkt,
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
                    })
                    running_qty = avg_entry = avg_exit = exit_qty_acc = Decimal("0")

                    # add leftover
                    leftover = total_abs - close_qty
                    leftover_signed = leftover if s_qty > 0 else -leftover
                    running_qty = leftover_signed
                    avg_entry = price
                    ttype_add = "ADD_L" if leftover_signed > 0 else "ADD_S"
                    out.append({
                        "market": mkt,
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
                    })

        return out

    fifo_rows = rows if is_fifo else raw_to_fifo(rows)

    # ---------- aggregate cycles ----------
    def aggregate_from_fifo(fifo_rows, output_path):
        # ✅ LOAD FUNDINGS HERE so it's in scope for flush()
        funding_path    = os.path.join("/root/arbSpread/backend/db_ext/raw/_fundings.csv")
        fundings        = load_fundings_csv(funding_path)

        def sk(r):
            t = r.get("readable_time","")
            try:
                return datetime.strptime(t.strip(), "%Y-%m-%d %H:%M:%S")
            except Exception:
                return t
        rows_sorted = sorted(fifo_rows, key=sk)

        out_rows = []
        in_cycle=False
        cycle_market=None
        cycle_side=None
        entry_time=None
        exit_time=None
        qty_opened=Decimal("0")
        qty_closed=Decimal("0")
        fees_sum=Decimal("0")
        pnl_sum=Decimal("0")
        net_sum=Decimal("0")
        entry_wsum=Decimal("0")
        exit_wsum=Decimal("0")
        entry_acc=Decimal("0")
        exit_acc=Decimal("0")

        def flush():
            nonlocal in_cycle, cycle_market, cycle_side, entry_time, exit_time
            nonlocal qty_opened, qty_closed, fees_sum, pnl_sum, net_sum
            nonlocal entry_wsum, exit_wsum, entry_acc, exit_acc
            if not in_cycle:
                return
            avg_entry = (entry_wsum/entry_acc) if entry_acc != 0 else Decimal("0")
            avg_exit  = (exit_wsum/exit_acc)  if exit_acc != 0 else Decimal("0")

            # determine if the cycle fully closed
            closed_all = (qty_opened != 0 and qty_closed == qty_opened)

            # parse times (entry always; exit only if closed)
            ts_entry = entry_time if isinstance(entry_time, datetime) else _parse_dt_jkt(entry_time or "")
            ts_exit  = (exit_time if isinstance(exit_time, datetime) else _parse_dt_jkt(exit_time or "")
                        ) if closed_all else None

            # funding only when fully closed (has exit boundary)
            funding_fees = funding_sum_for_window(
                fundings=fundings,
                market=cycle_market or "",
                t_start=ts_entry,
                t_end=ts_exit
            ) if closed_all else Decimal("0")

            net_incl_funding            = net_sum + funding_fees

            out_rows.append({
                "market": cycle_market or "",
                "entry_time"            : ts_entry.strftime("%Y-%m-%d %H:%M:%S") if ts_entry else (entry_time or ""),
                "exit_time"             :  ts_exit.strftime("%Y-%m-%d %H:%M:%S")  if ts_exit  else "",
                "qty_opened"            : str(qty_opened),
                "qty_closed"            : str(qty_closed if closed_all else Decimal("0")),
                "side"                  : cycle_side or "",
                "fees"                  : str(fees_sum if closed_all else Decimal("0")),
                "pnl"                   : str(pnl_sum if closed_all else Decimal("0")),
                "net_pnl"               : str(net_sum if closed_all else Decimal("0")),
                "avg_entry_price"       : str(avg_entry),
                "avg_exit_price"        : str(avg_exit if closed_all else Decimal("0")),
                "funding_fees"          : str(funding_fees if closed_all else Decimal("0")),
                "net_pnl_incl_funding"  : str(net_incl_funding if closed_all else Decimal("0")),
            })

            in_cycle=False
            cycle_market=None
            cycle_side=None
            entry_time=None
            exit_time=None
            qty_opened=Decimal("0")
            qty_closed=Decimal("0")
            fees_sum=Decimal("0")
            pnl_sum=Decimal("0")
            net_sum=Decimal("0")
            entry_wsum=Decimal("0")
            exit_wsum=Decimal("0")
            entry_acc=Decimal("0")
            exit_acc=Decimal("0")

        for r in rows_sorted:
            market = r["market"]
            ttype = r["trade_type"].strip().upper()
            qty = to_dec(r["qty"])
            price = to_dec(r["price"])
            fees = to_dec(r["fees"])
            pnl = to_dec(r["pnl"])
            net = to_dec(r["net_pnl"])
            rtime_str = r.get("readable_time","")
            try:
                rtime = datetime.strptime(rtime_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                rtime = rtime_str
            running_qty = to_dec(r.get("running_qty","0"))

            if not in_cycle:
                if ttype in ("ADD_L","ADD_S"):
                    in_cycle=True
                    cycle_market=market
                    cycle_side = "long" if ttype=="ADD_L" else "short"
                    entry_time = rtime
                    qty_opened = qty.copy_abs()
                    fees_sum = fees
                    pnl_sum = pnl
                    net_sum = net
                    entry_wsum = price * qty.copy_abs()
                    entry_acc = qty.copy_abs()
                    exit_time=None
                    exit_wsum=Decimal("0")
                    exit_acc=Decimal("0")
                    qty_closed=Decimal("0")
                else:
                    continue
            else:
                fees_sum += fees
                pnl_sum += pnl
                net_sum += net
                if ttype in ("ADD_L","ADD_S"):
                    qty_opened += qty.copy_abs()
                    entry_wsum += price * qty.copy_abs()
                    entry_acc += qty.copy_abs()
                elif ttype in ("REDUCE_L","REDUCE_S","CLOSE_L","CLOSE_S"):
                    closed = qty.copy_abs()
                    qty_closed += closed
                    exit_wsum += price * closed
                    exit_acc += closed
                    exit_time = rtime
                    if ttype.startswith("CLOSE") or running_qty == 0:
                        flush()
                else:
                    pass

        if in_cycle:
            flush()

        fieldnames = [
            "market","entry_time","exit_time","qty_opened","qty_closed","side",
            "fees","pnl","net_pnl","avg_entry_price","avg_exit_price",
            "funding_fees","net_pnl_incl_funding"   # NEW
        ]

        # --- at the end of aggregate_from_fifo(...) ---
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path,"w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for row in out_rows:
                w.writerow(row)

        return out_rows, fieldnames   # ✅ NEW: return for aggregator

        # --- at the very end of build_cycled_csv(...) ---
    return aggregate_from_fifo(fifo_rows, output_path)  # ✅ return rows+headers



import os, glob

def build_all_cycled_csvs():
    fifo_dir    = '/root/arbSpread/backend/db_ext/fifo'
    out_dir     = '/root/arbSpread/backend/db_ext/cycle'

    os.makedirs(out_dir, exist_ok=True)

    files = [
        f for f in sorted(glob.glob(os.path.join(fifo_dir, "*.csv")))
        if not os.path.basename(f).startswith("_")
    ]
    if not files:
        logger.info(f"⚠️ No CSV files found in {fifo_dir}")
        return

    count_ok = 0
    all_rows = []          # ✅ NEW
    merged_headers = None  # ✅ NEW

    for src in files:
        try:
            dst = os.path.join(out_dir, os.path.basename(src))
            rows, headers = build_cycled_csv(src, dst)   # ✅ capture returned rows
            if rows:
                all_rows.extend(rows)
                if not merged_headers:
                    merged_headers = headers
            count_ok += 1
        except Exception as e:
            logger.info(f"❌ Failed {src}: {e}")

    # ✅ Write merged file if anything collected
    if all_rows and merged_headers:
        # sort: empty exit_time first, then by exit_time DESC, tie-break by entry_time DESC
        def _parse_dt(s):
            try:
                return datetime.strptime((s or "").strip(), "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None

        def _key_exit_desc_empty_first(r):
            et = _parse_dt(r.get("exit_time", ""))
            if et is None:
                # empty exit_time → come first
                return (0, 0, 0)
            en = _parse_dt(r.get("entry_time", "")) or datetime.min
            # non-empty exit_time → after empty, sort by exit_time DESC, then entry_time DESC
            return (1, -et.timestamp(), -en.timestamp())

        all_rows.sort(key=_key_exit_desc_empty_first)

        merged_path = os.path.join(out_dir, "_allSymbols.csv")
        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=merged_headers)
            w.writeheader()
            for row in all_rows:
                w.writerow(row)
        logger.info(f"📦 Merged → {merged_path}")


import csv, os
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
from collections import defaultdict

getcontext().prec = 50

def build_daily_pnl(
    all_symbols_path="/root/arbSpread/backend/db_ext/cycle/_allSymbols.csv",
    out_path        ="/root/arbSpread/backend/db_ext/cycle/_sum.csv",
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
            ts_str          = row.get("exit_time", "")
            if not ts_str:
                continue
            dt_jkt = parse_jkt(ts_str)
            if dt_jkt is None:
                continue
            # convert to UTC date
            dt_utc          = dt_jkt - timedelta(hours=jkt_utc_offset_hours)
            key_date        = dt_utc.date().isoformat()  # "YYYY-MM-DD"


            qty_opened      = to_dec(row.get("qty_opened")) if row.get("exit_time") else Decimal("0")
            qty_closed      = to_dec(row.get("qty_closed")) if row.get("exit_time") else Decimal("0")
            avg_entry_price = to_dec(row.get("avg_entry_price")) if row.get("exit_time") else Decimal("0")
            avg_exit_price  = to_dec(row.get("avg_exit_price")) if row.get("exit_time") else Decimal("0")
            net_pnl         = to_dec(row.get("net_pnl_incl_funding")) if row.get("exit_time") else Decimal("0")

            qty             = qty_opened+qty_closed
            vol             = (qty_opened*avg_entry_price) + (qty_closed*avg_exit_price)

            # volume = sum(|qty| * price) (turnover)

            daily[key_date]["volume"]   += vol
            daily[key_date]["net"]      += net_pnl

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
