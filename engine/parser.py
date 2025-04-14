import os
import pandas as pd
from tqdm import tqdm
from engine.parser_cython import parse_file_cython

class ITCH:
    # … your __init__, helpers, outputs …

    def parse(self, filepath):
        # Show a byte‐level progress bar via tqdm
        total_bytes = os.path.getsize(filepath)
        pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc='Parsing ITCH')
        # Progress callback: update bar by delta
        def _progress(processed, total_size):
            pbar.update(processed - pbar.n)

        # Delegate entire parse to Cython with progress reporting
        order_book, executions, snapshots = parse_file_cython(filepath, callback=_progress)
        pbar.close()

        # Store back into self for any other logic
        self.order_book = order_book
        self.executions = executions

        # Write outputs
        self.output_order_book()
        df_exec = pd.DataFrame(self.executions)
        df_exec.to_csv("output/executions.csv", index=False)
        print("Executions saved to output/executions.csv")

        pd.DataFrame(snapshots).to_csv("output/snapshots.csv", index=False)
        print("Snapshots saved to output/snapshots.csv")