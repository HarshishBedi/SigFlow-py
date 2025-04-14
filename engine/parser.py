import os
import struct
import datetime
import pandas as pd

from book_snapshot import BookSnapshotter

class ITCH:
    def __init__(self):
        # Order book: key = order reference, value = dict with order details (stock, price, volume, side, timestamp)
        self.order_book = {}
        # Stock directory mapping: key = stock locate (or stock ID) to ticker
        self.stock_map = {}
        # List of executed trades (collected from order executions and trade messages)
        self.executions = []
        # For message processing that relies on hourly aggregation (e.g. trade messages)
        self.trades = []
        self.current_hour = None
        self.snapshotter = BookSnapshotter()
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

    # ----------------------------------------
    # Processing individual message types
    # ----------------------------------------

    def process_trade_P(self, msg):
        """
        Process a Trade (P) message according to Nasdaq TotalView-ITCH 5.0 specification.
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
        Returns a tuple: (trade_data, hour) where trade_data is [time, symbol, price, volume] and hour is the hour string.
        """
        if len(msg) != 43:
            print(f"Invalid trade message length: {len(msg)}")
            return None, None
        
        timestamp = int.from_bytes(msg[4:10], byteorder='big')
        volume = int.from_bytes(msg[19:23], byteorder='big')
        symbol = msg[23:31].decode('ascii').strip()
        price_int = int.from_bytes(msg[31:35], byteorder='big')
        price = price_int / 10000.0
        time_str = self.convert_timestamp(timestamp)
        hour = time_str.split(":")[0]
        return [time_str, symbol, price, volume], hour

    def process_add_order(self, msg, with_mpid=False):
        """
        Process an Add Order message.
        For type "A" (no MPID) and "F" (with MPID) messages.
        Format for "A": struct.unpack('>HH6sQcI8sI', msg)
        Format for "F": struct.unpack('>HH6sQcI8sI4s', msg)
        """
        if with_mpid:
            try:
                data = struct.unpack('>HH6sQcI8sI4s', msg)
            except struct.error as e:
                print("Add Order (F) unpack error:", e)
                return
        else:
            try:
                data = struct.unpack('>HH6sQcI8sI', msg)
            except struct.error as e:
                print("Add Order (A) unpack error:", e)
                return

        order_ref = data[3]
        shares = data[5]
        price = data[7] / 10000.0
        stock = data[6].decode('ascii').strip()
        # We ignore timestamp and tracking number for the order book state
        side = data[4].decode('ascii') if isinstance(data[4], bytes) else data[4]
        self.order_book[order_ref] = {
            'stock': stock,
            'price': price,
            'volume': shares,
            'side': side,
            'timestamp': None  # Optionally store timestamp if needed
        }
        # snapshot after add order
        ts = int.from_bytes(msg[4:10], byteorder='big')
        self.snapshotter.snapshot(self.order_book, ts, self.convert_timestamp)

    def process_order_replace(self, msg):
        """
        Process an Order Replace ("U") message.
        Format: struct.unpack('>HH6sQQII', msg)
        - Original order reference is in data[3]
        - New order reference is in data[4]
        - Shares (new quantity) in data[5]
        - Price in data[6] (divided by 10000)
        """
        try:
            data = struct.unpack('>HH6sQQII', msg)
        except struct.error as e:
            print("Order Replace unpack error:", e)
            return
        old_ref = data[3]
        new_ref = data[4]
        shares = data[5]
        price = data[6] / 10000.0
        if old_ref in self.order_book:
            order = self.order_book.pop(old_ref)
            order['volume'] = shares
            order['price'] = price
            self.order_book[new_ref] = order
            # snapshot after replace order
            ts = int.from_bytes(msg[4:10], byteorder='big')
            self.snapshotter.snapshot(self.order_book, ts, self.convert_timestamp)

    def process_order_delete(self, msg):
        """
        Process an Order Delete ("D") message.
        Format: struct.unpack('>HH6sQ', msg)
        Order Reference Number is in data[3].
        """
        try:
            data = struct.unpack('>HH6sQ', msg)
        except struct.error as e:
            print("Order Delete unpack error:", e)
            return
        order_ref = data[3]
        if order_ref in self.order_book:
            self.order_book.pop(order_ref)
            # snapshot after delete order
            ts = int.from_bytes(msg[4:10], byteorder='big')
            self.snapshotter.snapshot(self.order_book, ts, self.convert_timestamp)

    def process_order_executed(self, msg, with_price=False):
        """
        Process an Order Executed message.
        For type "E" (without execution price) and type "C" (with execution price).
        Format for "E": struct.unpack('>HH6sQIQ', msg)
        Format for "C": struct.unpack('>HH6sQIQcI', msg)
        For "E", price is taken from the order book.
        For "C", price is included and only counted if Printable field is "Y".
        """
        if with_price:
            try:
                data = struct.unpack('>HH6sQIQcI', msg)
            except struct.error as e:
                print("Order Executed with Price unpack error:", e)
                return
            printable = data[6].decode('ascii')
            if printable != "Y":
                return
            order_ref = data[3]
            quantity = data[4]
            price = data[7] / 10000.0
            timestamp = int.from_bytes(data[2], byteorder='big') if isinstance(data[2], bytes) else data[2]
        else:
            try:
                data = struct.unpack('>HH6sQIQ', msg)
            except struct.error as e:
                print("Order Executed unpack error:", e)
                return
            order_ref = data[3]
            quantity = data[4]
            timestamp = int.from_bytes(data[2], byteorder='big') if isinstance(data[2], bytes) else data[2]
            if order_ref in self.order_book:
                price = self.order_book[order_ref]['price']
            else:
                price = 0.0
        # Update the corresponding order in the book
        if order_ref in self.order_book:
            order = self.order_book[order_ref]
            if order['volume'] >= quantity:
                order['volume'] -= quantity
                if order['volume'] == 0:
                    self.order_book.pop(order_ref)
        # Record the execution trade
        executed = {
            'stock': self.order_book[order_ref]['stock'] if order_ref in self.order_book else "UNKNOWN",
            'price': price,
            'volume': quantity,
            'timestamp': self.convert_timestamp(timestamp)
        }
        self.executions.append(executed)
        # snapshot after order execution
        self.snapshotter.snapshot(self.order_book, timestamp, self.convert_timestamp)

    # ----------------------------------------
    # Parsing function
    # ----------------------------------------
    def parse(self, filepath):
        file_to_read = self._prepare_file(filepath)
        # Progress monitoring
        file_size = os.path.getsize(file_to_read)
        print(f"Starting parse of {file_to_read}, size {file_size/1e6:.2f} MB")
        message_count = 0
        total_bytes = 0
        
        # Define message sizes for each type (bytes)
        m_map = {
            b"S": 11,
            b"R": 38,
            b"H": 24,
            b"Y": 19,
            b"L": 25,
            b"V": 34,
            b"W": 11,
            b"K": 27,
            b"J": 34,
            b"h": 20,
            b"A": 35,   # Add Order (without MPID)
            b"F": 39,   # Add Order (with MPID)
            b"E": 30,   # Order Executed (no price)
            b"C": 35,   # Order Executed with price
            b"X": 22,
            b"D": 18,   # Order Delete
            b"U": 34,   # Order Replace
            b"P": 43,   # Trade Message
            b"Q": 39,
            b"B": 18,
            b"I": 49,
            b"N": 19
        }
        market_open = None
        market_close = None
        with open(file_to_read, "rb") as f:
            while True:
                header = f.read(1)
                if not header:
                    break
                size = m_map.get(header, None)
                if size is None:
                    continue
                msg = self.read_bytes(size, f)
                # Update progress
                message_count += 1
                total_bytes += 1 + size  # header + body
                if message_count % 1000000 == 0:
                    print(f"Processed {message_count} messages ({total_bytes/1e6:.1f} MB)")
                
                if header == b"S":
                    # System Event Message: determines market open/close
                    # Format: struct.unpack('>HH6sc', msg)
                    try:
                        data = struct.unpack('>HH6sc', msg)
                    except struct.error as e:
                        print("System Event unpack error:", e)
                        continue
                    event_code = data[3].decode()
                    timestamp = int.from_bytes(data[2], byteorder='big')
                    if event_code == "Q" and market_open is None:
                        market_open = timestamp
                        print(f"Market open at {market_open} ns")
                    elif event_code == "M":
                        market_close = timestamp
                        print(f"Market close at {market_close} ns")
                        break
                elif header == b"R":
                    # Stock Directory Message: map stock locate codes to ticker symbols
                    try:
                        data = struct.unpack('>HH6s8sccIcc2scccccIc', msg)
                    except struct.error as e:
                        print("Stock Directory unpack error:", e)
                        continue
                    stockID = data[0]
                    ticker = data[3].decode().strip()
                    self.stock_map[stockID] = ticker
                elif header == b"A":
                    self.process_add_order(msg, with_mpid=False)
                elif header == b"F":
                    self.process_add_order(msg, with_mpid=True)
                elif header == b"U":
                    self.process_order_replace(msg)
                elif header == b"D":
                    self.process_order_delete(msg)
                elif header == b"E":
                    self.process_order_executed(msg, with_price=False)
                elif header == b"C":
                    self.process_order_executed(msg, with_price=True)
                elif header == b"P":
                    # Process Trade Message (non-cross)
                    trade_data, trade_hour = self.process_trade_P(msg)
                    if trade_data:
                        # For simplicity, record these trades in executions as well
                        self.executions.append({
                            'stock': trade_data[1],
                            'price': trade_data[2],
                            'volume': trade_data[3],
                            'timestamp': trade_data[0]
                        })
                # Additional message types can be handled similarly
        # Output the final order book and execution trade data
        self.output_order_book()
        self.output_executions()
        # output snapshots to CSV
        self.snapshotter.output("output/snapshots.csv")
        print(f"Parsing complete: {message_count} messages, {total_bytes/1e6:.2f} MB processed")

    def output_order_book(self):
        orders = []
        for ref, order in self.order_book.items():
            orders.append({
                'order_ref': ref,
                'stock': order['stock'],
                'price': order['price'],
                'volume': order['volume'],
                'side': order['side'],
                'timestamp': order['timestamp']
            })
        df = pd.DataFrame(orders)
        df.to_csv("output/order_book.csv", index=False)
        print("Order book saved to output/order_book.csv")

    def output_executions(self):
        df = pd.DataFrame(self.executions)
        df.to_csv("output/executions.csv", index=False)
        print("Executions saved to output/executions.csv")

def main(fileName):
    parser = ITCH()
    parser.parse(fileName)

if __name__ == '__main__':
    # Set the desired ITCH binary file from data/unzipped
    fileName = os.path.join('data', 'unzipped', '10302019.NASDAQ_ITCH50.bin')
    main(fileName)