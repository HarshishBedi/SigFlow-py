import pandas as pd

class BookSnapshotter:
    def __init__(self):
        # will hold dicts: {'timestamp':…, 'best_bid_price':…, etc.}
        self.snapshots = []

    def snapshot(self, order_book, ts_ns, convert_timestamp):
        """
        Capture the top‑of‑book from `order_book` at nanosecond timestamp `ts_ns`.
        `convert_timestamp` is a callable that turns ts_ns→"HH:MM:SS".
        """
        time_str = convert_timestamp(ts_ns)
        bids = [(o['price'], o['volume']) for o in order_book.values() if o['side']=='B']
        asks = [(o['price'], o['volume']) for o in order_book.values() if o['side']=='S']
        best_bid_price, best_bid_vol = max(bids) if bids else (None, None)
        best_ask_price, best_ask_vol = min(asks) if asks else (None, None)

        self.snapshots.append({
            'timestamp': time_str,
            'best_bid_price': best_bid_price,
            'best_bid_volume': best_bid_vol,
            'best_ask_price': best_ask_price,
            'best_ask_volume': best_ask_vol
        })

    def output(self, path="output/snapshots.csv"):
        """Write all collected snapshots to CSV."""
        df = pd.DataFrame(self.snapshots)
        df.to_csv(path, index=False)
        print(f"Snapshots saved to {path}")