import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'tt2_data.db')

def _get_tickers_from_db(source='sp500', limit=None):
    """Fetches a list of active stock symbols from the database."""
    with sqlite3.connect(DB_PATH) as conn:
        query = f"SELECT symbol FROM assets WHERE is_active = 1 AND source = '{source}'"
        if limit:
            query += f" LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
    return df['symbol'].tolist()


def fetch(scope='top_10_sp500'):
    """
    Fetches historical price data for assets and calculates a suite of technical metrics.
    This version uses yf.download() for greater speed and reliability.
    """
    print(f"Fetching and calculating daily metrics with scope: '{scope}'")
    
    if scope == 'top_10_sp500':
        tickers_to_process = _get_tickers_from_db(source='sp500', limit=10)
    else:
        tickers_to_process = _get_tickers_from_db(source='sp500')

    if not tickers_to_process:
        print("No tickers found in the database to process.")
        return pd.DataFrame()

    print(f"Found {len(tickers_to_process)} tickers. Starting bulk download...")

    # ❗ Change: auto_adjust=False to keep Volume data
    try:
        data = yf.download(
            tickers=tickers_to_process,
            period="1y",
            group_by='ticker',
            auto_adjust=False,
            threads=True
        )
    except Exception as e:
        print(f"    ❌ Bulk download failed: {e}")
        return pd.DataFrame()

    if data.empty:
        print("No data returned from yfinance bulk download.")
        return pd.DataFrame()

    all_metrics_dfs = []
    print("Download complete. Calculating metrics for each ticker...")
    
    for symbol in tickers_to_process:
        hist = data[symbol].copy()

        # Flatten any multi-level columns
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.droplevel()
        if hist.columns.name:
            hist.columns.name = None

        hist.dropna(how='all', inplace=True)
        if hist.empty:
            continue

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
        ]]
        metrics_df.dropna(inplace=True)
        all_metrics_dfs.append(metrics_df)
    
    if not all_metrics_dfs:
        print("⚠️ No valid dataframes generated.")
        return pd.DataFrame()
    
    # Combine all tickers
    master_df = pd.concat(all_metrics_dfs)
    master_df['date'] = pd.to_datetime(master_df['date']).dt.strftime('%Y-%m-%d')

    # ✅ Convert all numpy types to native Python types for SQLite compatibility
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

    # Cast to native Python scalars (not numpy)
    master_df = master_df.applymap(lambda x: float(x) if isinstance(x, (np.float32, np.float64)) else x)

    print(f"✅ Successfully calculated {len(master_df)} total daily metric records.")
    print(master_df.head())
    print(master_df.dtypes)

    return master_df