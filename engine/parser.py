"""
Parser for NASDAQ ITCH 5.0 files.

Reads a compressed ITCH file from the data/raw directory, parses trade messages,
calculates running VWAP per hour, and writes the results to the data/output directory.
"""

import sys
import os
import gzip
import struct
import pandas as pd
from tqdm import tqdm
import argparse
import re

def parse_args():
    parser = argparse.ArgumentParser(description="Parse ITCH and compute VWAP for custom time range and granularity.")
    parser.add_argument("fileName", help="Path to ITCH file")
    parser.add_argument("--time_from", default="09:30", help="Start time in HH:MM (default: 09:30)")
    parser.add_argument("--time_to", default="16:00", help="End time in HH:MM (default: 16:00)")
    parser.add_argument("--granularity", type=str, default="3600s", help="Granularity with time unit (ns, us, ms, s), e.g. '3600s' (default: '3600s')")
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Only include data for this stock ticker"
    )
    return parser.parse_args()

def parse_granularity(gran_str):
    import re
    m = re.match(r'([\d\.]+)(ns|us|ms|s)?$', gran_str.strip())
    if not m:
        raise ValueError(f"Invalid granularity format: {gran_str}")
    value, unit = m.groups()
    value = float(value)
    if unit is None or unit == "s":
        seconds = value
    elif unit == "ms":
        seconds = value / 1000.0
    elif unit == "us":
        seconds = value / 1_000_000.0
    elif unit == "ns":
        seconds = value / 1_000_000_000.0
    return seconds

def time_str_to_ns(tstr):
    h, m = map(int, tstr.split(":"))
    return (h*3600 + m*60) * 1_000_000_000

def messageMap():
    """
    Return mapping of NASDAQ ITCH message types to their byte lengths.
    """
    m_map = dict()
    m_map["S"] = 11 
    m_map["R"] = 38 
    m_map["H"] = 24
    m_map["Y"] = 19
    m_map["L"] = 25
    m_map["V"] = 34
    m_map["W"] = 11
    m_map["K"] = 27
    m_map["J"] = 34
    m_map["h"] = 20
    m_map["A"] = 35 
    m_map["F"] = 39 
    m_map["E"] = 30 
    m_map["C"] = 35 
    m_map["X"] = 22
    m_map["D"] = 18
    m_map["U"] = 34 
    m_map["P"] = 43 
    m_map["Q"] = 39
    m_map["B"] = 18
    m_map["I"] = 49
    m_map["N"] = 19
    return m_map

def bucketMap(stockIDs, start_ns, end_ns, gran_ns):
    """
    Initialize trade buckets for each stock ID given custom time range and granularity.
    """
    trades = {}
    for ID in stockIDs:
        stock_trades = {}
        t = start_ns
        while t < end_ns:
            stock_trades[t] = (0, 0)
            t += gran_ns
        trades[ID] = stock_trades
    return trades

def calculate_bucket(time, start_ns, gran_ns):
    """
    Determine the bucket start time for a timestamp given granularity.
    """
    offset = time - start_ns
    bucket_start = (offset // gran_ns) * gran_ns + start_ns
    return bucket_start

def read_bytes(file, n):
    """
    Read n bytes from the file and return the raw data.
    """
    message = file.read(n)
    return message

def decodeTimestamp(timestamp):
    """
    Convert a 6-byte timestamp to an unsigned 8-byte integer.
    """
    new_bytes = struct.pack('>2s6s',b'\x00\x00', timestamp) 
    new_timestamp = struct.unpack('>Q', new_bytes)
    return new_timestamp[0]

def hourlyMap(stockIDs, openTime, endTime):
    """
    Initialize hourly trade buckets for each stock ID.
    """
    trades = dict()
    nsPerHour = (10**9) * (60*60) 
    for ID in stockIDs:
        stock_trades = dict()
        for n in range(10, 17):
            hour = "%d:00" % ((n + 11) % 12 + 1)
            tradeTuple = (0, 0) 
            stock_trades[hour] = tradeTuple
        trades[ID] = stock_trades
    return trades

def calculateHour(time, endTime):
    """
    Determine the hourly bucket label for a given timestamp.
    """
    nsPerHour = (10**9) * (60*60) 
    hour = "%d:00" % ((min(16 - ((endTime - time) // nsPerHour),
        16) + 11) % 12 + 1)
    return hour

def parseOrders(trades, orders, endTime):
    """
    Aggregate trade orders into hourly VWAP calculation buckets.
    """
    for order in orders:
        stock, price, quantity, time = order
        hour = calculateHour(time, endTime)

        curValue, curQuantity = trades[stock][hour]
        value = price * quantity
        curValue += value
        curQuantity += quantity

        trades[stock][hour] = (curValue, curQuantity)
    return trades

def parseTrades(file, m_map, start_ns, end_ns, gran_ns):
    """
    Parse the ITCH file, extract trade messages within [start_ns, end_ns), and bucket them by granularity.
    """
    added_orders = {}
    filled_orders = []
    stock_map = {}

    file.seek(0)
      # Initialize progress bar
    try:
        total_bytes = os.path.getsize(file.name)
    except Exception:
        total_bytes = None
    if total_bytes:
        pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc='Parsing ITCH')
    else:
          pbar = None

    while True:
        m_type = file.read(1)
        if not m_type:
            break
        m_type = m_type.decode()
        if m_type not in m_map:
            continue
        steps = m_map[m_type]
        msg = read_bytes(file, steps)
        if pbar:
            pbar.update(1)

        if m_type == "R":
            data = struct.unpack('>HH6s8sccIcc2scccccIc', msg)
            stockID = data[0]
            ticker = data[3].decode().strip()
            stock_map[stockID] = ticker
        elif m_type in ("A", "F"):
            fmt = '>HH6sQcI8sI' if m_type == "A" else '>HH6sQcI8sI4s'
            data = struct.unpack(fmt, msg)
            reference = data[3]
            price = data[7] / (10 ** 4)
            added_orders[reference] = price
        elif m_type in ("E", "C", "P"):
            if m_type == "E":
                data = struct.unpack('>HH6sQIQ', msg)
                reference = data[3]
                quantity = data[4]
                time = decodeTimestamp(data[2])
                price = added_orders.get(reference, 0)
                stockID = data[0]
            elif m_type == "C":
                data = struct.unpack('>HH6sQIQcI', msg)
                if data[6].decode() != "Y":
                    continue
                stockID = data[0]
                quantity = data[4]
                time = decodeTimestamp(data[2])
                price = data[7] / (10 ** 4)
            else:  # "P"
                data = struct.unpack('>HH6sQcIQIQ', msg)
                stockID = data[0]
                quantity = data[5]
                time = decodeTimestamp(data[2])
                price = data[7] / (10 ** 4)

            if time < start_ns:
                continue
            if time >= end_ns:
                break
            bucket = calculate_bucket(time, start_ns, gran_ns)
            filled_orders.append((stockID, price, quantity, bucket))
            if pbar:
                pbar.update(steps)

    trades = bucketMap(stock_map.keys(), start_ns, end_ns, gran_ns)
    for stockID, price, quantity, bucket in filled_orders:
        curValue, curQuantity = trades[stockID][bucket]
        trades[stockID][bucket] = (curValue + price * quantity, curQuantity + quantity)

    if pbar:
        pbar.close()

    return stock_map, trades

def VWAP(tradeTuple, runningValue = 0, runningQuantity = 0):
    """
    Calculate the running Volume Weighted Average Price (VWAP).
    """
    totalValue, totalQuantity = tradeTuple
    totalValue += runningValue
    totalQuantity += runningQuantity
    if totalQuantity != 0:
        average = totalValue/totalQuantity
    else:
        average = 0
    return totalValue, totalQuantity, average

def ns_to_time(ns):
    """Convert a nanosecond timestamp to a human-readable time string with sub-second precision if needed."""
    total_seconds = ns / 1_000_000_000
    hours = int(total_seconds) // 3600
    minutes = (int(total_seconds) % 3600) // 60
    seconds = int(total_seconds) % 60
    fraction = total_seconds - int(total_seconds)
    if fraction > 0:
        # Format fraction up to 9 decimal places and strip trailing zeros
        frac_str = f"{fraction:.9f}"[2:].rstrip("0")
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{frac_str}"
    else:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def main(fileName, time_from, time_to, granularity, ticker=None):
    """
    Entry point: parse the given ITCH file and export hourly VWAP CSV.
    """
    # Convert time range and granularity to nanoseconds
    start_ns = time_str_to_ns(time_from)
    end_ns = time_str_to_ns(time_to)
    seconds_val = parse_granularity(granularity)
    gran_ns = int(seconds_val * 1_000_000_000)

    os.makedirs('data/output', exist_ok=True)
    # Determine output CSV path
    base_name = os.path.splitext(os.path.basename(fileName))[0]
    outName = os.path.join('data/output', f"{base_name}.csv")

    file = gzip.open(fileName, 'rb')
    m_map = messageMap() 
    stock_map, trades = parseTrades(file, m_map, start_ns, end_ns, gran_ns)

    # If a specific ticker was requested, filter trades to that ticker only
    if ticker:
        matching_ids = [sid for sid, name in stock_map.items() if name == ticker]
        if not matching_ids:
            raise ValueError(f"Ticker '{ticker}' not found in data.")
        trades = {sid: trades[sid] for sid in matching_ids}

    # Prepare output using custom time range and granularity
    # Determine bucket start times and labels
    sorted_buckets = sorted(next(iter(trades.values())).keys())
    bucket_labels = {bucket: ns_to_time(bucket) for bucket in sorted_buckets}
    
    # Build data for DataFrame
    data = {"Stock Ticker": []}
    for bucket in sorted_buckets:
        label = bucket_labels[bucket]
        data[label] = []

    # Compute running VWAP for each stock over buckets
    for stockID, bucket_dict in trades.items():
        data["Stock Ticker"].append(stock_map[stockID])
        runningValue = 0
        runningQuantity = 0
        for bucket in sorted_buckets:
            value, qty = bucket_dict[bucket]
            runningValue += value
            runningQuantity += qty
            avg = runningValue / runningQuantity if runningQuantity else 0
            data[bucket_labels[bucket]].append(avg)
    # Create DataFrame and write CSV
    output = pd.DataFrame(data)
    output.to_csv(outName, index=False)

def testCalculateHour():
    """
    Test the calculateHour function with sample timestamps.
    """
    startTime = 34200000036157
    endTime = 57600000113132
    testTime = 57500000113132
    print(calculateHour(testTime, endTime))
    return

if __name__ == "__main__":
    args = parse_args()
    main(args.fileName, args.time_from, args.time_to, args.granularity, args.ticker)