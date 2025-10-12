import yfinance as yf
import pandas as pd
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'tt2_data.db')


def _get_tickers_from_db(source='sp500', limit=None):
    """Fetches a list of active stock symbols from the database."""
    with sqlite3.connect(DB_PATH) as conn:
        query = f"SELECT symbol FROM assets WHERE is_active = 1 AND source = '{source}'"
        if limit:
            query += f" LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
    return df['symbol'].tolist()


def _fetch_single_earnings(symbol):
    """Fetch earnings date info for a single symbol."""
    try:
        ticker = yf.Ticker(symbol)

        if hasattr(ticker, "get_earnings_dates"):
            df = ticker.get_earnings_dates(limit=8)

        else:
            df = ticker.earnings_dates

        if df is not None and not df.empty:
            df = df.reset_index().rename(columns={
                'index': 'earnings_date',
                'EPS Estimate': 'eps_estimate',
                'Reported EPS': 'eps_reported',
                'Surprise(%)': 'eps_surprise_pct'
            })
            df['symbol'] = symbol
            return df
    except Exception as e:
        print(f"[WARN] Failed to fetch earnings data for {symbol}: {e}")
    return None


def fetch(scope='top_10_sp500', max_workers=10):
    """Fetches and aggregates upcoming and past earnings dates for a set of tickers."""
    print(f"Fetching earnings dates with scope: '{scope}'")

    if scope == 'top_10_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=10)
    elif scope == 'top_250_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=250)
    elif scope == 'sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500')
    elif isinstance(scope, list):
        tickers_to_check = scope
    else:
        print(f"Error: Unknown scope '{scope}'. Aborting earnings fetch.")
        return pd.DataFrame()

    if not tickers_to_check:
        print("No tickers found in database to process.")
        return pd.DataFrame()

    all_earnings_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_earnings, sym): sym for sym in tickers_to_check}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching earnings data"):
            result = future.result()
            if result is not None:
                all_earnings_data.append(result)

    if not all_earnings_data:
        print("No earnings data found for specified scope.")
        return pd.DataFrame()

    master_df = pd.concat(all_earnings_data, ignore_index=True)

    master_df['earnings_date'] = pd.to_datetime(master_df['earnings_date']).dt.strftime('%Y-%m-%d')
    master_df = master_df.astype({
        "eps_estimate": float,
        "eps_reported": float,
        "eps_surprise_pct": float
    })

    print(f"Successfully fetched {len(master_df)} total earnings records.")
    return master_df


if __name__ == "__main__":
    df = fetch(scope='top_250_sp500', max_workers=20)
    print(df.head())

