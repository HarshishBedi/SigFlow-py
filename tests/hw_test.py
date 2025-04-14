import os
import struct
import datetime
import pytest
import pandas as pd
from pathlib import Path

# Import the ITCH class from your hourly_vwap module.
from engine.hourly_vwap import ITCH

def create_dummy_trade(timestamp, volume, price, symbol=b'AAPL'):
    """
    Create a dummy Trade (P) message in Nasdaq TotalView-ITCH 5.0 format.
    The message body is 43 bytes and structured as follows:
      - Bytes 0-1: Stock Locate (2 bytes, unused, set to zeros)
      - Bytes 2-3: Tracking Number (2 bytes, unused, set to zeros)
      - Bytes 4-9: Timestamp (6 bytes, nanoseconds since midnight)
      - Bytes 10-17: Order Reference Number (8 bytes, unused, set to zeros)
      - Byte 18: Buy/Sell Indicator (1 byte, unused, e.g., b'X')
      - Bytes 19-22: Volume (4 bytes, represents trade volume)
      - Bytes 23-30: Stock Symbol (8 bytes, ASCII, right-padded with spaces)
      - Bytes 31-34: Price (4 bytes, unsigned int; actual price = value/10000)
      - Bytes 35-42: Match Number (8 bytes, unused, set to zeros)
    """
    dummy = bytearray(43)
    # Set timestamp at offset 4 (6 bytes)
    dummy[4:10] = timestamp.to_bytes(6, byteorder='big')
    # Set volume at offset 19 (4 bytes)
    dummy[19:23] = volume.to_bytes(4, byteorder='big')
    # Set symbol at offset 23 (8 bytes), padded with spaces
    sym_bytes = symbol.ljust(8, b' ')
    dummy[23:31] = sym_bytes
    # Set price at offset 31 (4 bytes)
    price_int = int(price * 10000)
    dummy[31:35] = price_int.to_bytes(4, byteorder='big')
    # Other fields remain zeros
    return bytes(dummy)

@pytest.fixture
def change_to_tmp_dir(tmp_path, monkeypatch):
    """
    Change working directory to tmp_path. This isolates output and input files.
    """
    monkeypatch.chdir(tmp_path)
    return tmp_path

def write_dummy_file(tmp_path, messages, subfolder="data/unzipped", filename="dummy.bin"):
    """
    Write a list of message bytes (each including a 1-byte header followed by the 43-byte message)
    to a dummy file in the specified subfolder of tmp_path.
    Returns the Path to the created dummy file.
    """
    target_dir = tmp_path / subfolder
    target_dir.mkdir(parents=True, exist_ok=True)
    dummy_file = target_dir / filename
    # Write all messages consecutively
    with open(dummy_file, "wb") as f:
        for m in messages:
            f.write(m)
    return dummy_file

def test_single_trade(change_to_tmp_dir, tmp_path):
    """
    Test with a single valid trade message.
    Expect that an output file (for hour "00") is produced with correct details.
    """
    # Use timestamp 0 -> 00:00:00
    timestamp = 0
    trade_msg = create_dummy_trade(timestamp, volume=100, price=150.0, symbol=b'AAPL')
    full_message = b'P' + trade_msg

    dummy_file = write_dummy_file(tmp_path, [full_message])
    parser = ITCH()
    parser.parse(str(dummy_file))
    
    output_file = Path("output") / "00.txt"
    assert output_file.exists(), "Expected output file for hour '00' does not exist."
    df = pd.read_csv(output_file, sep=" ")
    row = df.iloc[0]
    assert row["time"] == "00:00:00", f"Expected time '00:00:00', got {row['time']}"
    assert row["symbol"].strip() == "AAPL", f"Expected symbol 'AAPL', got {row['symbol']}"
    assert abs(row["vwap"] - 150.0) < 0.01, f"Expected VWAP 150.0, got {row['vwap']}"

def test_multiple_trades_same_hour(change_to_tmp_dir, tmp_path):
    """
    Test with multiple trades all in the same hour.
    Expect that the VWAP is the weighted average of the trades.
    """
    # Use timestamp 0 -> 00:00:00 for both messages
    timestamp = 0
    trade1 = create_dummy_trade(timestamp, volume=100, price=150.0, symbol=b'AAPL')
    trade2 = create_dummy_trade(timestamp, volume=200, price=155.0, symbol=b'AAPL')
    msg1 = b'P' + trade1
    msg2 = b'P' + trade2
    dummy_file = write_dummy_file(tmp_path, [msg1, msg2])
    
    parser = ITCH()
    parser.parse(str(dummy_file))
    
    output_file = Path("output") / "00.txt"
    assert output_file.exists(), "Expected output file for hour '00' does not exist."
    df = pd.read_csv(output_file, sep=" ")
    # Expected VWAP = ((150.0*100)+(155.0*200)) / (100+200) = (15000+31000)/300 ≈ 153.33
    expected_vwap = 153.33
    row = df.iloc[0]
    assert row["time"] == "00:00:00", f"Expected time '00:00:00', got {row['time']}"
    assert row["symbol"].strip() == "AAPL", f"Expected symbol 'AAPL', got {row['symbol']}"
    assert abs(row["vwap"] - expected_vwap) < 0.01, f"Expected VWAP {expected_vwap}, got {row['vwap']}"

def test_multiple_trades_different_hours(change_to_tmp_dir, tmp_path):
    """
    Test with trades occurring in two different hours.
    Expect output files for each hour with their respective VWAP values.
    """
    # First trade with timestamp corresponding to hour "00"
    t1 = 0  # 00:00:00
    # Second trade with timestamp corresponding to hour "01"
    # For example, 1 hour = 3600 seconds, in nanoseconds
    t2 = 3600 * 10**9
    trade1 = create_dummy_trade(t1, volume=100, price=150.0, symbol=b'AAPL')
    trade2 = create_dummy_trade(t2, volume=100, price=160.0, symbol=b'AAPL')
    msg1 = b'P' + trade1
    msg2 = b'P' + trade2
    dummy_file = write_dummy_file(tmp_path, [msg1, msg2])
    
    parser = ITCH()
    parser.parse(str(dummy_file))
    
    # Check for hour "00" output
    output_file_00 = Path("output") / "00.txt"
    assert output_file_00.exists(), "Expected output file for hour '00' does not exist."
    df00 = pd.read_csv(output_file_00, sep=" ")
    row_00 = df00.iloc[0]
    # For hour 00, only one trade exists with VWAP = 150.0
    assert row_00["time"] == "00:00:00", f"Expected time '00:00:00', got {row_00['time']}"
    assert row_00["symbol"].strip() == "AAPL", f"Expected symbol 'AAPL', got {row_00['symbol']}"
    assert abs(row_00["vwap"] - 150.0) < 0.01, f"Expected VWAP 150.0, got {row_00['vwap']}"
    
    # Check for hour "01" output
    output_file_01 = Path("output") / "01.txt"
    assert output_file_01.exists(), "Expected output file for hour '01' does not exist."
    df01 = pd.read_csv(output_file_01, sep=" ")
    row_01 = df01.iloc[0]
    # For hour 01, the trade's price is 160.0, so VWAP should be 160.0
    assert row_01["time"] == "01:00:00", f"Expected time '01:00:00', got {row_01['time']}"
    assert row_01["symbol"].strip() == "AAPL", f"Expected symbol 'AAPL', got {row_01['symbol']}"
    assert abs(row_01["vwap"] - 160.0) < 0.01, f"Expected VWAP 160.0, got {row_01['vwap']}"

def test_invalid_trade_message(change_to_tmp_dir, tmp_path):
    """
    Test with an invalid trade message (incorrect length).
    Expect that the parser does not crash and does not produce any output.
    """
    # Create an invalid trade message (wrong length, e.g., only 30 bytes after header)
    invalid_trade = b'\x00' * 30  # insufficient length
    full_msg = b'P' + invalid_trade
    dummy_file = write_dummy_file(tmp_path, [full_msg])
    
    parser = ITCH()
    parser.parse(str(dummy_file))
    
    # Since the message is invalid, there should be no output files generated.
    output_dir = Path("output")
    txt_files = list(output_dir.glob("*.txt"))
    assert len(txt_files) == 0, "Expected no output files for invalid trade message."

if __name__ == '__main__':
    # Run all tests when executed directly (for quick debugging)
    test_single_trade(change_to_tmp_dir(pytest.ensuretemp("tmp_dir")), pytest.ensuretemp("tmp_dir"))
    test_multiple_trades_same_hour(change_to_tmp_dir(pytest.ensuretemp("tmp_dir")), pytest.ensuretemp("tmp_dir"))
    test_multiple_trades_different_hours(change_to_tmp_dir(pytest.ensuretemp("tmp_dir")), pytest.ensuretemp("tmp_dir"))
    test_invalid_trade_message(change_to_tmp_dir(pytest.ensuretemp("tmp_dir")), pytest.ensuretemp("tmp_dir"))