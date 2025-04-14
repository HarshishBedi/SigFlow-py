import os
import struct
import datetime
import pandas as pd

class ITCH:
    def __init__(self):
        # List to accumulate trade messages (each is [time, symbol, price, volume])
        self.trades = []
        # Current hour for which trades are accumulating (as string, e.g. "00")
        self.current_hour = None
        # Ensure output directory exists
        os.makedirs("output", exist_ok=True)

    def _prepare_file(self, filepath):
        """
        Assumes that the input ITCH file is already an unzipped binary 
        (from the data/unzipped folder) and returns the filepath unmodified.
        """
        return filepath

    def read_bytes(self, size, f):
        """Read a specific number of bytes from the file object."""
        return f.read(size)

    def convert_timestamp(self, ts):
        """Convert a nanosecond timestamp to a HH:MM:SS string using UTC time with a timezone-aware datetime."""
        dt = datetime.datetime.fromtimestamp(ts / 1e9, tz=datetime.timezone.utc)
        return dt.strftime('%H:%M:%S')

    def process_trade(self, msg):
        """
        Unpack a Trade (P) message according to Nasdaq TotalView-ITCH 5.0 specification.
        Expected message body is 43 bytes:
          - Bytes 0-1: Stock Locate (2 bytes, unused)
          - Bytes 2-3: Tracking Number (2 bytes, unused)
          - Bytes 4-9: Timestamp (6 bytes, nanoseconds since midnight)
          - Bytes 10-17: Order Reference Number (8 bytes, unused)
          - Byte 18: Buy/Sell Indicator (1 byte, unused)
          - Bytes 19-22: Shares (4 bytes, represents volume)
          - Bytes 23-30: Stock Symbol (8 bytes, ASCII, right-padded)
          - Bytes 31-34: Price (4 bytes, unsigned int; divided by 10000 for the actual value)
          - Bytes 35-42: Match Number (8 bytes, unused)
        Returns a tuple: (trade_data, hour) where trade_data is [time, symbol, price, volume] and hour is the hour string (e.g. "00").
        """
        if len(msg) != 43:
            print(f"Invalid trade message length: {len(msg)}")
            return None, None
        
        # Extract a 6-byte timestamp from bytes 4 to 10
        timestamp = int.from_bytes(msg[4:10], byteorder='big')
        
        # Extract volume (shares) from bytes 19 to 23
        volume = int.from_bytes(msg[19:23], byteorder='big')
        
        # Extract symbol from bytes 23 to 31 and strip right-padding
        symbol = msg[23:31].decode('ascii').strip()
        
        # Extract price from bytes 31 to 35 and calculate actual price
        price_int = int.from_bytes(msg[31:35], byteorder='big')
        price = price_int / 10000.0
        
        # Convert timestamp to a UTC time string using the updated conversion method
        time_str = self.convert_timestamp(timestamp)
        hour = time_str.split(":")[0]
        
        return [time_str, symbol, price, volume], hour

    def cal_vwap(self, trades):
        """
        Calculate Volume Weighted Average Price (VWAP) given a list of trade messages.
        Each trade message is a list: [time, symbol, price, volume].
        """
        df = pd.DataFrame(trades, columns=["time", "symbol", "price", "volume"])
        df["amount"] = df["price"] * df["volume"]
        # Convert time string to a datetime object
        df["time"] = pd.to_datetime(df["time"], format="%H:%M:%S")
        # Create a new column 'hour'
        df["hour"] = df["time"].dt.hour
        # Group by hour and symbol
        grouped = df.groupby(["hour", "symbol"], as_index=False).agg({"amount": "sum", "volume": "sum"})
        grouped["vwap"] = (grouped["amount"] / grouped["volume"]).round(2)
        # Format hour as a time string (e.g. "00:00:00")
        grouped["time"] = grouped["hour"].apply(lambda h: f"{int(h):02d}:00:00")
        return grouped[["time", "symbol", "vwap"]]

    def flush_trades(self, hour):
        """
        Process all accumulated trades for a given hour and write the result to an output file.
        """
        if self.trades:
            result_df = self.cal_vwap(self.trades)
            out_file = os.path.join("output", f"{hour}.txt")
            result_df.to_csv(out_file, sep=" ", index=False)
            print(f"Output written for hour {hour} in file {out_file}")
            # Reset the trades list after flushing
            self.trades = []

    def parse(self, filepath):
        """
        Parse the ITCH binary file.
          - Reads each message using a record size lookup.
          - Processes only Trade messages (type 'P') to build VWAP aggregates.
          - When the hour changes, flushes the accumulated trades.
        """
        file_to_read = self._prepare_file(filepath)
        # Mapping of message headers (as bytes) to record sizes (in bytes)
        record_sizes = {
            b"S": 11,
            b"R": 38,
            b"H": 24,
            b"Y": 19,
            b"L": 25,
            b"V": 34,
            b"W": 11,
            b"K": 27,
            b"A": 35,
            b"F": 39,
            b"E": 30,
            b"C": 35,
            b"X": 22,
            b"D": 18,
            b"U": 34,
            b"P": 43,
            b"Q": 39,
            b"B": 18,
            b"I": 49,
            b"N": 19
        }
        with open(file_to_read, "rb") as f:
            while True:
                # Read the message type header (1 byte)
                header = f.read(1)
                if not header:
                    break
                size = record_sizes.get(header, None)
                if size is None:
                    # Skip unknown message types
                    continue
                record = self.read_bytes(size, f)
                if header == b"P":
                    trade_data, trade_hour = self.process_trade(record)
                    if trade_data is None:
                        continue
                    if self.current_hour is None:
                        self.current_hour = trade_hour
                    # If the current message's hour differs from the current accumulating hour,
                    # flush accumulated trades and update the current hour.
                    if trade_hour != self.current_hour:
                        self.flush_trades(self.current_hour)
                        self.current_hour = trade_hour
                    self.trades.append(trade_data)
        # Flush any remaining trades when file ends
        if self.trades:
            self.flush_trades(self.current_hour)

if __name__ == '__main__':
    # Adjust the file path to fetch the ITCH file from the data/unzipped folder.
    filepath = os.path.join('data', 'unzipped', '10302019.NASDAQ_ITCH50.bin')
    parser = ITCH()
    parser.parse(filepath)