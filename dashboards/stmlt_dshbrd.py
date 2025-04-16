"""
VWAP Dashboard for Single Stock
"""
import os
import streamlit as st
import pandas as pd
from pandas.errors import ParserError
import altair as alt
import datetime
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from engine.parser import main as parse_main

@st.cache_data
def load_data(path: str) -> pd.DataFrame:
    """
    Load VWAP CSV and return a DataFrame.
    """
    try:
        df = pd.read_csv(path)
    except ParserError:
        # Fallback to python engine and skip malformed lines
        df = pd.read_csv(path, engine="python", on_bad_lines="skip")
    # Clean column names: strip whitespace and remove BOM characters
    df.columns = df.columns.str.strip().str.replace('\ufeff', '')
    return df


def main():
    """
    Streamlit app: display running VWAP time series for a selected stock.
    """
    st.title("SigFlow - VWAP Dashboard")
    st.write("Harshish Singh Bedi, 2025")
    st.sidebar.title("Settings")

    output_dir = os.path.join("data", "output")
    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    date_bases = [f.split(".")[0] for f in csv_files]
    def format_date(x):
        return datetime.datetime.strptime(x, "%m%d%Y").strftime("%B %d, %Y")
    selected_base = st.sidebar.selectbox("Select Date", date_bases, format_func=format_date)
    formatted_date = format_date(selected_base)

    # Determine paths for raw and output files
    raw_dir = os.path.join("data", "raw")
    raw_files = [f for f in os.listdir(raw_dir) if f.startswith(selected_base + ".")]
    raw_path = os.path.join(raw_dir, raw_files[0]) if raw_files else None
    data_filename = next((f for f in os.listdir(output_dir) if f.startswith(selected_base + ".")), None)
    data_path = os.path.join(output_dir, data_filename) if data_filename else None

    # Parser parameters
    if "parsing" not in st.session_state:
        st.session_state["parsing"] = False
    st.sidebar.subheader("Parser Parameters")
    time_from = st.sidebar.text_input("Start time (HH:MM)", "09:30")
    time_to   = st.sidebar.text_input("End time (HH:MM)", "09:35")
    # Allow pandas Timedelta alias for granularity (ns, us, ms, s)
    gran_col1, _ = st.sidebar.columns([3, 1])
    granularity = gran_col1.text_input("Granularity (e.g. '1ns', '100us', '1ms', '1s')", "1s")
    ticker_input = st.sidebar.text_input("Ticker (leave blank for all)", "AAPL")
    if st.sidebar.button("Run Parser", disabled=st.session_state.get("parsing", False)):
        st.session_state["parsing"] = True
        with st.spinner("Running parser..."):
            if raw_path:
                try:
                    parse_main(raw_path, time_from, time_to, granularity, ticker_input or None)
                    st.success("Parsing complete! Data refreshed.")
                    load_data.clear()
                except Exception as e:
                    st.error(f"Parser error: {e}")
            else:
                st.error("Raw file for selected date not found.")
        st.session_state["parsing"] = False

    # Load data or stop if missing
    if data_path:
        df = load_data(data_path)
    else:
        st.warning("Data file for selected date not found.")
        st.stop()

    # Stop if DataFrame is empty
    if df.empty:
        st.warning("No data loaded for selected date.")
        st.stop()

    # Ensure 'Stock Ticker' column exists
    if "Stock Ticker" not in df.columns:
        st.error(f"'Stock Ticker' column not found. Available columns: {', '.join(df.columns)}")
        st.stop()

    # Sidebar: select a single ticker
    tickers = df["Stock Ticker"].unique().tolist()
    ticker = st.sidebar.selectbox("Select Stock Ticker", tickers)

    # Filter and prepare VWAP series
    vwap_cols = [col for col in df.columns if col != "Stock Ticker"]
    vwap_series = df.loc[df["Stock Ticker"] == ticker, vwap_cols].iloc[0]
    vwap_df = vwap_series.to_frame(name="VWAP")

    # Check if vwap_df is empty or if the VWAP column contains only NaN values
    if vwap_df.empty or vwap_df["VWAP"].isna().all():
        st.warning("No data available for the selected parameters.")
    else:
        # Compute y-axis domain for proper zoom
        y_min = float(vwap_df["VWAP"].min() - 0.1)
        y_max = float(vwap_df["VWAP"].max() + 0.1)
        chart_data = vwap_df.reset_index().rename(columns={"index": "Hour"})
        # Ensure Hour is a string and clean up the label
        chart_data["Hour"] = chart_data["Hour"].astype(str).str.replace(r" Running VWAP", "", regex=True)
        # Replace any empty strings in the 'Hour' column with a default value "Unknown"
        chart_data["Hour"] = chart_data["Hour"].replace("", "Unknown")
        chart = alt.Chart(chart_data).mark_line(point=True).encode(
            x=alt.X("Hour:N", title="Hour"),
            y=alt.Y("VWAP:Q", scale=alt.Scale(domain=[y_min, y_max]), title="VWAP"),
            tooltip=["Hour", "VWAP"]
        ).interactive()
        st.subheader(f"Running VWAP for {ticker} on {formatted_date}")
        st.altair_chart(chart, use_container_width=True)
        st.subheader("Parsed Data")
        st.dataframe(df)


if __name__ == "__main__":
    main()