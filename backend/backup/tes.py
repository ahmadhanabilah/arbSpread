import csv
from datetime import datetime, timedelta
INPUT       = "/root/arbSpread/backend/db_lig/fifo/_allSymbols.csv"
OUTPUT      = "_tes.csv"
TARGET_DATE = "2025-10-10"               # date in UTC (after conversion)
JKT_OFFSET  = timedelta(hours=7)         # JKT = UTC+7

def to_utc(jkt_string):
    """convert 'YYYY-MM-DD HH:MM:SS' (JKT) → datetime in UTC"""
    try:
        jkt = datetime.strptime(jkt_string, "%Y-%m-%d %H:%M:%S")
        return jkt - JKT_OFFSET
    except:
        return None

with open(INPUT, newline="", encoding="utf-8") as f_in, \
     open(OUTPUT, "w", newline="", encoding="utf-8") as f_out:

    reader = csv.DictReader(f_in)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(f_out, fieldnames=fieldnames)
    writer.writeheader()

    count = 0
    for row in reader:
        ts_jkt = row.get("readable_time", "")
        ts_utc = to_utc(ts_jkt)
        if not ts_utc:
            continue

        # match only rows that are UTC date 2025-05-04
        if ts_utc.strftime("%Y-%m-%d") == TARGET_DATE:
            writer.writerow(row)
            count += 1

print(f"Done. Extracted {count} rows for {TARGET_DATE} UTC → {OUTPUT}")
