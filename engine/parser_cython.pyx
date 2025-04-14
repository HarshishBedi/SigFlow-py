# distutils: extra_compile_args = -Xpreprocessor -fopenmp
# distutils: extra_link_args = -lomp
# cython: boundscheck=False, wraparound=False

import mmap
import struct
import datetime
from engine.book_snapshot import BookSnapshotter

# Pre-build the message-size map as a Cython dict for fast lookup
cdef dict M_MAP = {
    b'S': 11, b'R': 38, b'H': 24, b'Y': 19, b'L': 25, b'V': 34,
    b'W': 11, b'K': 27, b'J': 34, b'h': 20, b'A': 35, b'F': 39,
    b'E': 30, b'C': 35, b'X': 22, b'D': 18, b'U': 34, b'P': 43,
    b'Q': 39, b'B': 18, b'I': 49, b'N': 19
}

def parse_file_cython(file_path, callback=None):
    """
    Cython-accelerated ITCH v5.0 parser.
    Returns (order_book_dict, executions_list, snapshots_list).
    Calls `callback(processed_bytes, total_bytes)` periodically if provided.
    """
    cdef object snapshotter = BookSnapshotter()
    cdef dict order_book = {}
    cdef list executions = []
    cdef Py_ssize_t i, file_len
    cdef object buf
    cdef bytes header
    cdef int size
    cdef bytes msg

    # memory-map file
    f = open(file_path, 'rb')
    try:
        buf = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        file_len = buf.size()
    finally:
        f.close()

    # setup progress reporting: every 0.1% of file
    cdef Py_ssize_t report_interval = file_len // 1000 if file_len >= 1000 else 1
    cdef Py_ssize_t next_report = report_interval

    i = 0
    while i < file_len:
        header = buf[i:i+1]
        size = M_MAP.get(header, 0)
        if size == 0:
            i += 1
            continue
        msg = buf[i+1:i+1+size]

        # Add Order (A/F)
        if header == b'A' or header == b'F':
            if header == b'A':
                stock_loc, trk, ts_bytes, order_ref, side_byte, shares, sym, price_i = struct.unpack('>HH6sQcI8sI', msg)
            else:
                stock_loc, trk, ts_bytes, order_ref, side_byte, shares, sym, price_i, _ = struct.unpack('>HH6sQcI8sI4s', msg)
            # normalize side
            if hasattr(side_byte, 'decode'):
                side_char = side_byte.decode('ascii')
            else:
                side_char = chr(side_byte)
            order_book[order_ref] = {
                'stock': sym.decode('ascii').strip(),
                'price': price_i/10000.0,
                'volume': shares,
                'side': side_char,
                'timestamp': None
            }
            ts = int.from_bytes(ts_bytes, 'big')
            if callback and i >= next_report:
                callback(i, file_len)
                next_report += report_interval
            snapshotter.snapshot(order_book, ts, lambda t: datetime.datetime.fromtimestamp(t/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S'))

        # Replace Order (U)
        elif header == b'U':
            stock_loc, trk, ts_bytes, old_ref, new_ref, shares, price_i = struct.unpack('>HH6sQQII', msg)
            if old_ref in order_book:
                order = order_book.pop(old_ref)
                order['volume'] = shares
                order['price'] = price_i/10000.0
                order_book[new_ref] = order
                ts = int.from_bytes(ts_bytes, 'big')
                if callback and i >= next_report:
                    callback(i, file_len)
                    next_report += report_interval
                snapshotter.snapshot(order_book, ts, lambda t: datetime.datetime.fromtimestamp(t/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S'))

        # Delete Order (D)
        elif header == b'D':
            stock_loc, trk, ts_bytes, ref = struct.unpack('>HH6sQ', msg)
            if ref in order_book:
                order_book.pop(ref)
                ts = int.from_bytes(ts_bytes, 'big')
                if callback and i >= next_report:
                    callback(i, file_len)
                    next_report += report_interval
                snapshotter.snapshot(order_book, ts, lambda t: datetime.datetime.fromtimestamp(t/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S'))

        # Executed no-price (E)
        elif header == b'E':
            stock_loc, trk, ts_bytes, ref, qty, match = struct.unpack('>HH6sQIQ', msg)
            price = order_book.get(ref, {}).get('price', 0.0)
            ts = int.from_bytes(ts_bytes, 'big')
            if ref in order_book:
                order_book[ref]['volume'] -= qty
                if order_book[ref]['volume'] == 0:
                    order_book.pop(ref)
            executions.append({
                'stock': order_book.get(ref, {}).get('stock', 'UNKNOWN'),
                'price': price,
                'volume': qty,
                'timestamp': datetime.datetime.fromtimestamp(ts/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S')
            })
            if callback and i >= next_report:
                callback(i, file_len)
                next_report += report_interval
            snapshotter.snapshot(order_book, ts, lambda t: datetime.datetime.fromtimestamp(t/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S'))

        # Executed with price (C)
        elif header == b'C':
            stock_loc, trk, ts_bytes, ref, qty, match, printable, price_i = struct.unpack('>HH6sQIQcI', msg)
            if printable.decode('ascii') == 'Y':
                price = price_i/10000.0
                ts = int.from_bytes(ts_bytes, 'big')
                if ref in order_book:
                    order_book[ref]['volume'] -= qty
                    if order_book[ref]['volume'] == 0:
                        order_book.pop(ref)
                executions.append({
                    'stock': order_book.get(ref, {}).get('stock', 'UNKNOWN'),
                    'price': price,
                    'volume': qty,
                    'timestamp': datetime.datetime.fromtimestamp(ts/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S')
                })
                if callback and i >= next_report:
                    callback(i, file_len)
                    next_report += report_interval
                snapshotter.snapshot(order_book, ts, lambda t: datetime.datetime.fromtimestamp(t/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S'))

        # Trade Message (P)
        elif header == b'P':
            ts = int.from_bytes(msg[4:10], 'big')
            qty = int.from_bytes(msg[19:23], 'big')
            symbol = msg[23:31].decode('ascii').strip()
            price = int.from_bytes(msg[31:35], 'big')/10000.0
            tstr = datetime.datetime.fromtimestamp(ts/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S')
            executions.append({'stock': symbol, 'price': price, 'volume': qty, 'timestamp': tstr})
            if callback and i >= next_report:
                callback(i, file_len)
                next_report += report_interval
            snapshotter.snapshot(order_book, ts, lambda t: datetime.datetime.fromtimestamp(t/1e9, tz=datetime.timezone.utc).strftime('%H:%M:%S'))

        i += 1 + size

    buf.close()
    return order_book, executions, snapshotter.snapshots