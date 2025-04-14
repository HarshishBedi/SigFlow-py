# engine/backtester.py
import pandas as pd

def run_backtest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run a basic backtest using generated signals.
    
    Strategy:
    - Compute simple returns from the price series.
    - Use the previous period's signal (position) to capture lag effects.
    - Calculate strategy returns and cumulative returns over time.
    """
    df = df.copy()
    
    # Ensure data is sorted by timestamp
    if "timestamp" in df.columns:
        df.sort_values("timestamp", inplace=True)
    
    # Calculate returns on price
    df['return'] = df['price'].pct_change().fillna(0)
    
    # Shift signal to apply it to the next period's return
    df['position'] = df['signal'].shift(1).fillna(0)
    df['strategy_return'] = df['position'] * df['return']
    
    # Compute cumulative strategy returns
    df['cumulative_return'] = (1 + df['strategy_return']).cumprod()
    
    return df