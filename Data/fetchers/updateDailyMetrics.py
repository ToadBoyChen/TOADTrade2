import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
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


def _process_single_ticker(symbol, data):
    """Compute daily metrics for a single symbol from bulk-downloaded data."""
    try:
        hist = data[symbol].copy()

        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.droplevel()
        if hist.columns.name:
            hist.columns.name = None

        hist.dropna(how='all', inplace=True)
        if hist.empty:
            return None

        log_return = np.log(hist['Close'] / hist['Close'].shift(1))
        hist['volatility_30d'] = log_return.rolling(window=30).std() * np.sqrt(252)
        hist.ta.sma(length=20, append=True, col_names=('ma_20d',))
        hist.ta.sma(length=50, append=True, col_names=('ma_50d',))
        hist.ta.rsi(length=14, append=True, col_names=('rsi_14d',))

        hist.reset_index(inplace=True)
        hist['asset_symbol'] = symbol

        hist.rename(columns={
            'Date': 'date', 'Open': 'open', 'High': 'high',
            'Low': 'low', 'Close': 'close', 'Volume': 'volume'
        }, inplace=True)

        metrics_df = hist[[
            'asset_symbol', 'date', 'open', 'high', 'low', 'close', 'volume',
            'volatility_30d', 'ma_20d', 'ma_50d', 'rsi_14d'
        ]].dropna()

        return metrics_df

    except Exception as e:
        print(f"[WARN] Failed to process {symbol}: {e}")
        return None


def fetch(scope='top_10_sp500', max_workers=10):
    """
    Fetches historical price data for assets and calculates a suite of technical metrics.
    Now uses ThreadPoolExecutor and tqdm for progress tracking.
    """
    print(f"Fetching and calculating daily metrics with scope: '{scope}'")

    if scope == 'top_10_sp500':
        tickers_to_process = _get_tickers_from_db(source='sp500', limit=10)
    elif scope == 'top_250_sp500':
        tickers_to_process = _get_tickers_from_db(source='sp500', limit=250)
    elif scope == 'sp500':
        tickers_to_process = _get_tickers_from_db(source='sp500')
    elif isinstance(scope, list):
        tickers_to_process = scope
    else:
        print(f"Error: Unknown scope '{scope}'. Aborting metric fetch.")
        return pd.DataFrame()

    if not tickers_to_process:
        print("No tickers found in the database to process.")
        return pd.DataFrame()

    print(f"Found {len(tickers_to_process)} tickers. Starting bulk download...")

    try:
        data = yf.download(
            tickers=tickers_to_process,
            period="1y",
            group_by='ticker',
            auto_adjust=False,
            threads=True
        )
    except Exception as e:
        print(f"Bulk download failed: {e}")
        return pd.DataFrame()

    if data.empty:
        print("No data returned from yfinance bulk download.")
        return pd.DataFrame()

    print("Download complete. Calculating metrics in parallel...")

    all_metrics_dfs = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_ticker, symbol, data): symbol for symbol in tickers_to_process}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Calculating metrics"):
            result = future.result()
            if result is not None:
                all_metrics_dfs.append(result)

    if not all_metrics_dfs:
        print("No valid metric dataframes generated.")
        return pd.DataFrame()

    master_df = pd.concat(all_metrics_dfs, ignore_index=True)
    master_df['date'] = pd.to_datetime(master_df['date']).dt.strftime('%Y-%m-%d')

    # Convert numpy types for SQLite compatibility
    master_df = master_df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "volatility_30d": float,
        "ma_20d": float,
        "ma_50d": float,
        "rsi_14d": float
    })

    master_df = master_df.applymap(lambda x: float(x) if isinstance(x, (np.float32, np.float64)) else x)

    print(f"Successfully calculated {len(master_df)} total daily metric records.")
    return master_df


if __name__ == "__main__":
    df = fetch(scope='top_250_sp500', max_workers=20)
    print(df.head())

