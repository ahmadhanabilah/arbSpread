import csv
import os
import logging
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger("trade_processor")
logger.setLevel(logging.INFO)

# Define the fields for the final merged trade summary
FINAL_FIELDNAMES = [
    "trade_id", 
    "original_group_id", 
    "source", 
    "symbol", 
    "side", 
    "total_qty", 
    "weighted_avg_price", 
    "total_value", 
    "total_fee", 
    "execution_time_start",
    "fill_count"
]

def _read_csv(filepath: str) -> List[Dict[str, str]]:
    """Helper function to read a CSV file into a list of dictionaries."""
    if not os.path.exists(filepath):
        logger.warning(f"File not found: {filepath}")
        return []

    data = []
    with open(filepath, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data

def _process_groups(raw_data: List[Dict[str, str]], source_name: str, grouping_key_fields: List[str], trade_map: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Groups, aggregates, and calculates WAP for a list of raw trade fills.
    
    :param trade_map: A dictionary mapping raw field names to aggregated field names.
    """
    
    # 1. Grouping: Create a nested dictionary for aggregation
    grouped_trades = {}
    
    for row in raw_data:
        # Create a unique key for grouping (e.g., 'AVNT:tx_hash_xyz' or 'AVNT-USD:order_id_123')
        group_id = ":".join([row[key] for key in grouping_key_fields])
        
        if group_id not in grouped_trades:
            grouped_trades[group_id] = {
                "fills": [],
                "source": source_name,
                "symbol": row[trade_map["symbol_raw"]].split('-')[0], # Clean symbol
                "side": row[trade_map["side_raw"]],
                "total_qty": 0.0,
                "total_value": 0.0,
                "total_fee": 0.0,
                "earliest_time_str": row[trade_map["time_raw"]],
                "price_x_qty_sum": 0.0,
            }
        
        group = grouped_trades[group_id]
        
        try:
            qty = float(row[trade_map["qty_raw"]])
            price = float(row[trade_map["price_raw"]])
            value = float(row[trade_map["value_raw"]])
            fee = float(row.get(trade_map.get("fee_raw"), 0.0)) # Use .get for optional fields

            # Check for the earliest timestamp
            if row[trade_map["time_raw"]] < group["earliest_time_str"]:
                group["earliest_time_str"] = row[trade_map["time_raw"]]

            # Accumulate totals
            group["total_qty"] += qty
            group["total_value"] += value
            group["total_fee"] += fee
            group["price_x_qty_sum"] += (price * qty)
            group["fills"].append(row)

        except (ValueError, KeyError) as e:
            logger.error(f"Skipping trade fill due to data error in {source_name}: {e} in row {row}")
            continue

    # 2. Aggregation & WAP Calculation
    final_trades = []
    trade_counter = 1 
    
    for group_id, data in grouped_trades.items():
        if data["total_qty"] == 0:
            logger.warning(f"Skipping group with zero total quantity: {group_id}")
            continue

        weighted_avg_price = data["price_x_qty_sum"] / data["total_qty"]
        
        final_trades.append({
            "trade_id": f"{source_name[:3].upper()}_{trade_counter}",
            "original_group_id": group_id.split(":")[1], # Use only the tx_hash/order_id part
            "source": data["source"],
            "symbol": data["symbol"],
            "side": data["side"],
            "total_qty": round(data["total_qty"], 8),
            "weighted_avg_price": round(weighted_avg_price, 8),
            "total_value": round(data["total_value"], 8),
            "total_fee": round(data["total_fee"], 8),
            "execution_time_start": data["earliest_time_str"],
            "fill_count": len(data["fills"])
        })
        trade_counter += 1

    return final_trades


def process_lighter_trades():
    """Reads trades_lig.csv, groups by (symbol, tx_hash), and saves to finalTrades_lig.csv."""
    logger.info("Starting Lighter trade processing...")
    raw_data = _read_csv("trades_lig.csv")
    
    # Define mapping for Lighter
    LIG_MAP = {
        "symbol_raw": "symbol",
        "side_raw": "side",
        "qty_raw": "size",
        "price_raw": "price",
        "value_raw": "usd_amount",
        "time_raw": "timestamp",
        # "fee_raw" is missing/0 in Lighter data, handled by .get in _process_groups
    }
    
    # Grouping key: symbol + transaction hash (tx_hash)
    final_trades = _process_groups(raw_data, "LIGHTER", ["symbol", "tx_hash"], LIG_MAP)
    
    filename = "finalTrades_lig.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_FIELDNAMES)
        writer.writeheader()
        writer.writerows(final_trades)

    logger.info(f"✅ Lighter processing complete. Saved {len(final_trades)} final trades → {filename}")


def process_extended_trades():
    """Reads trades_ext.csv, groups by (market, order_id), and saves to finalTrades_ext.csv."""
    logger.info("Starting Extended trade processing...")
    raw_data = _read_csv("trades_ext.csv")
    
    # Define mapping for Extended
    EXT_MAP = {
        "symbol_raw": "market",
        "side_raw": "side",
        "qty_raw": "qty",
        "price_raw": "price",
        "value_raw": "value",
        "fee_raw": "fee",
        "time_raw": "created_at",
    }
    
    # Grouping key: market + order ID (order_id)
    final_trades = _process_groups(raw_data, "EXTENDED", ["market", "order_id"], EXT_MAP)
    
    filename = "finalTrades_ext.csv"
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_FIELDNAMES)
        writer.writeheader()
        writer.writerows(final_trades)

    logger.info(f"✅ Extended processing complete. Saved {len(final_trades)} final trades → {filename}")
