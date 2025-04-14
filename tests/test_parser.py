import os
import struct
import datetime
import pytest
import pandas as pd
from pathlib import Path

from engine.parser import ITCH

def create_dummy_trade(timestamp, volume, price, symbol=b'AAPL'):
    """
    Create a dummy Trade (P) message according to ITCH v5.0.
    The message body is 43 bytes:
      - Bytes 0-1: Stock Locate (2 bytes, zeros)
      - Bytes 2-3: Tracking Number (2 bytes, zeros)
      - Bytes 4-9: Timestamp (6 bytes, nanoseconds since midnight)
      - Bytes 10-17: Order Reference Number (8 bytes, zeros)
      - Byte 18: Buy/Sell Indicator (1 byte, e.g., b'X')
      - Bytes 19-22: Volume (4 bytes)
      - Bytes 23-30: Stock Symbol (8 bytes, ASCII, right-padded)
      - Bytes 31-34: Price (4 bytes, unsigned; actual price = value / 10000)
      - Bytes 35-42: Match Number (8 bytes, zeros)
    """
    dummy = bytearray(43)
    # Timestamp: offset 4 (6 bytes)
    dummy[4:10] = timestamp.to_bytes(6, byteorder='big')
    # Volume: offset 19 (4 bytes)
    dummy[19:23] = volume.to_bytes(4, byteorder='big')
    # Stock Symbol: offset 23 (8 bytes)
    sym_bytes = symbol.ljust(8, b' ')
    dummy[23:31] = sym_bytes
    # Price: offset 31 (4 bytes)
    price_int = int(price * 10000)
    dummy[31:35] = price_int.to_bytes(4, byteorder='big')
    # Remaining bytes (order ref, match number, etc.) remain zero.
    return bytes(dummy)

def write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="dummy_feed.bin"):
    """
    Write a list of message bytes (each message is the 1-byte header + message body)
    into a file in the specified subfolder of tmp_path.
    Returns the Path of the created file.
    """
    target_dir = tmp_path / subfolder
    target_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = target_dir / filename
    with open(dummy_file, "wb") as f:
        for m in messages:
            f.write(m)
    return dummy_file

@pytest.fixture
def change_to_tmp_dir(tmp_path, monkeypatch):
    """
    Change the working directory to tmp_path.
    This isolates the test so that all I/O (including output) occurs in tmp_path.
    """
    monkeypatch.chdir(tmp_path)
    return tmp_path

def test_extended_parser(change_to_tmp_dir, tmp_path):
    """
    Test the extended ITCH parser by simulating a feed that contains:
      1. An S message marking market open (event code "Q")
      2. An R message mapping stock ID 1 to ticker "AAPL"
      3. An A message to add an order for AAPL
      4. A P message (trade) for AAPL
      5. An S message marking market close (event code "M")
    Then verifies that the parser produces the output files:
      - "output/order_book.csv" containing the current order book state
      - "output/executions.csv" containing the recorded executions/trades.
    """
    messages = []
    
    # 1. S message: System Event for market open.
    # Format '>HH6sc': stock locate (H), tracking (H), timestamp 6 bytes, event code (c)
    s_open_body = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, byteorder='big'), b'Q')
    messages.append(b'S' + s_open_body)
    
    # 2. R message: Stock Directory, mapping stock id 1 -> "AAPL"
    # Format: '>HH6s8sccIcc2scccccIc' (38 bytes total).
    # Provide minimal valid data: stockID=1, ticker="AAPL    " and fill rest with spaces/zeros.
    r_body = struct.pack('>HH6s8sccIcc2scccccIc',
                         1,                    # stockID
                         0,                    # tracking number
                         b'\x00'*6,            # timestamp
                         b'AAPL    ',          # stock symbol, 8 bytes
                         b' ',                 # c
                         b' ',                 # c
                         0,                    # I: dummy integer
                         b' ',                 # c
                         b' ',                 # c
                         b'  ',                # 2s
                         b' ',                 # c
                         b' ',                 # c
                         b' ',                 # c
                         b' ',                 # c
                         b' ',                 # c
                         0,                    # I: dummy int
                         b' '                  # c
                        )
    messages.append(b'R' + r_body)
    
    # 3. A message: Add Order without MPID.
    # Format for "A": '>HH6sQcI8sI' (35 bytes total)
    a_body = struct.pack('>HH6sQcI8sI',
                         1,                    # stock locate (should match stock ID in R)
                         0,                    # tracking number
                         b'\x00'*6,            # timestamp (dummy)
                         100,                  # order reference number (e.g., 100)
                         b'B',                 # buy/sell indicator, e.g., 'B'
                         100,                  # shares (volume)
                         b'AAPL    ',          # stock symbol
                         1500000               # price, representing 150.0000 after dividing by 10000
                        )
    messages.append(b'A' + a_body)
    
    # 4. P message: Trade message.
    # We'll create a trade with timestamp 0, volume=100, price=150.0 for AAPL.
    trade_body = create_dummy_trade(0, volume=100, price=150.0, symbol=b'AAPL')
    messages.append(b'P' + trade_body)
    
    # 5. S message: System Event for market close, event code "M".
    s_close_body = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, byteorder='big'), b'M')
    messages.append(b'S' + s_close_body)
    
    # Write the dummy feed to a file in data/unzipped.
    dummy_feed = write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="dummy_feed.bin")
    
    # Instantiate and run the parser.
    parser = ITCH()
    parser.parse(str(dummy_feed))
    
    # Check that output files are created.
    order_book_file = Path("output") / "order_book.csv"
    executions_file = Path("output") / "executions.csv"
    assert order_book_file.exists(), "order_book.csv not found."
    assert executions_file.exists(), "executions.csv not found."
    
    # Read and test the executions CSV.
    df_exec = pd.read_csv(executions_file)
    # We expect at least the trade from the P message to have been recorded.
    assert not df_exec.empty, "Executions CSV is empty."
    # Check that one of the executions corresponds to the trade for AAPL with price 150.0 and volume 100.
    aapl_exec = df_exec[df_exec['stock'] == 'AAPL']
    assert not aapl_exec.empty, "No execution for AAPL found in executions.csv."
    # Since our trade timestamp is 0, the converted time should be "00:00:00".
    exec_row = aapl_exec.iloc[0]
    assert exec_row['timestamp'] == "00:00:00", f"Expected timestamp '00:00:00', got {exec_row['timestamp']}"
    assert abs(exec_row['price'] - 150.0) < 0.01, f"Expected price 150.0, got {exec_row['price']}"
    assert exec_row['volume'] == 100, f"Expected volume 100, got {exec_row['volume']}"
    
    # Optionally, check order_book.csv if desired. For instance, the add order may still be present depending on execution.
    df_book = pd.read_csv(order_book_file)
    # In this simple case, our executed trade may have reduced the order volume
    # (or even removed the order if fully executed), so the exact expected state may vary.
    print("Order book entries:", df_book.shape[0])
    print("Executions entries:", df_exec.shape[0])
    
def test_multiple_trades(change_to_tmp_dir, tmp_path):
    # Simulate two P (trade) messages in one feed
    messages = []
    # Market open
    s_open = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'Q')
    messages.append(b'S' + s_open)
    # Two trades for AAPL
    t1 = create_dummy_trade(1_000_000_000, volume=50, price=100.5, symbol=b'AAPL')
    t2 = create_dummy_trade(2_000_000_000, volume=75, price=200.25, symbol=b'AAPL')
    messages.append(b'P' + t1)
    messages.append(b'P' + t2)
    # Market close
    s_close = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'M')
    messages.append(b'S' + s_close)

    dummy_feed = write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="multi_trades.bin")
    parser = ITCH()
    parser.parse(str(dummy_feed))

    df_exec = pd.read_csv(Path("output") / "executions.csv")
    assert df_exec.shape[0] == 2, f"Expected 2 trades, got {df_exec.shape[0]}"
    assert set(df_exec['volume']) == {50, 75}


def test_trade_fractional_price(change_to_tmp_dir, tmp_path):
    # Test fractional price conversion
    messages = []
    s_open = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'Q')
    messages.append(b'S' + s_open)
    # Trade with fractional price
    trade = create_dummy_trade(0, volume=123, price=150.5678, symbol=b'AAPL')
    messages.append(b'P' + trade)
    s_close = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'M')
    messages.append(b'S' + s_close)

    dummy_feed = write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="frac_price.bin")
    parser = ITCH()
    parser.parse(str(dummy_feed))

    df_exec = pd.read_csv(Path("output") / "executions.csv")
    price = df_exec.iloc[0]['price']
    assert abs(price - 150.5678) < 1e-4, f"Expected price ~150.5678, got {price}"


def test_trade_timestamp_conversion(change_to_tmp_dir, tmp_path):
    # Test timestamp nanoseconds to HH:MM:SS conversion
    messages = []
    s_open = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'Q')
    messages.append(b'S' + s_open)
    # 1 hour, 1 minute, 1 second = 3661 seconds -> nanoseconds
    ts = (1*3600 + 1*60 + 1) * 1_000_000_000
    trade = create_dummy_trade(ts, volume=10, price=10.0, symbol=b'AAPL')
    messages.append(b'P' + trade)
    s_close = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'M')
    messages.append(b'S' + s_close)

    dummy_feed = write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="timestamp.bin")
    parser = ITCH()
    parser.parse(str(dummy_feed))

    df_exec = pd.read_csv(Path("output") / "executions.csv")
    assert df_exec.iloc[0]['timestamp'] == "01:01:01"


def test_symbol_padding(change_to_tmp_dir, tmp_path):
    # Ensure symbol padding/trimming is handled
    messages = []
    s_open = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'Q')
    messages.append(b'S' + s_open)
    # Symbol 'XYZ' padded to 8 bytes
    trade = create_dummy_trade(0, volume=20, price=20.0, symbol=b'XYZ')
    messages.append(b'P' + trade)
    s_close = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'M')
    messages.append(b'S' + s_close)

    dummy_feed = write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="symbol_pad.bin")
    parser = ITCH()
    parser.parse(str(dummy_feed))

    df_exec = pd.read_csv(Path("output") / "executions.csv")
    assert df_exec.iloc[0]['stock'] == 'XYZ'


def test_unknown_message_type(change_to_tmp_dir, tmp_path):
    # Unknown message types should be skipped without error
    messages = []
    s_open = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'Q')
    messages.append(b'S' + s_open)
    # Unknown header 'Z' with dummy payload
    messages.append(b'Z' + b'\x00'*10)
    s_close = struct.pack('>HH6sc', 0, 0, (0).to_bytes(6, 'big'), b'M')
    messages.append(b'S' + s_close)

    dummy_feed = write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="unknown.bin")
    parser = ITCH()
    parser.parse(str(dummy_feed))

    exec_file = Path("output") / "executions.csv"
    assert exec_file.exists(), "executions.csv not found."
    try:
        df_exec = pd.read_csv(exec_file)
    except pd.errors.EmptyDataError:
        df_exec = pd.DataFrame()
    assert df_exec.empty, "Expected no executions for unknown message types"

if __name__ == '__main__':
    # For quick debugging, run the test directly.
    test_extended_parser(change_to_tmp_dir(pytest.ensuretemp("tmp_dir")), pytest.ensuretemp("tmp_dir"))