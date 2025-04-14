# main.py
import pandas as pd
from signals.moving_average import generate_moving_average_signal
from engine.backtester import run_backtest

def load_data(file_path: str) -> pd.DataFrame:
    """
    Load market data from a CSV file.
    Expects a CSV with at least 'timestamp' and 'price' columns.
    """
    try:
        df = pd.read_csv(file_path, parse_dates=['timestamp'])
        print(f"Loaded {len(df)} records from {file_path}")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        exit(1)

def main():
    # 1. Load data
    data_file = "data/raw/market_data.csv"
    print("Loading data...")
    df = load_data(data_file)

    # 2. Generate signals (using a simple moving-average crossover strategy)
    print("Generating signals...")
    df_signals = generate_moving_average_signal(df, short_window=5, long_window=10)

    # 3. Run backtest
    print("Running backtest...")
    results = run_backtest(df_signals)

    # 4. Output some sample results
    print("Backtesting complete. Here are the first few rows of the results:")
    print(results.head())

if __name__ == "__main__":
    main()