import yfinance as yf
import pandas as pd
import sqlite3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'tt2_data.db')


def _get_tickers_from_db(source='sp500', limit=None):
    with sqlite3.connect(DB_PATH) as conn:
        query = f"SELECT symbol FROM assets WHERE is_active = 1 AND source = '{source}'"
        if limit:
            query += f" LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
    return df['symbol'].tolist()


def _fetch_single_insider(symbol):
    try:
        ticker = yf.Ticker(symbol)
        transactions = ticker.insider_transactions

        if transactions is not None and not transactions.empty:
            transactions['symbol'] = symbol
            return transactions
    except Exception as e:
        print(f"[WARN] Failed to fetch insider info for {symbol}: {e}")
    return None


def fetch(scope='top_10_sp500', max_workers=10):
    if scope == 'top_10_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=10)
    elif scope == 'top_250_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=250)
    elif scope == 'sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500')
    elif isinstance(scope, list):
        tickers_to_check = scope
    else:
        print(f"Error: Unknown scope '{scope}'. Aborting insider fetch.")
        return pd.DataFrame()

    all_insider_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_single_insider, sym): sym for sym in tickers_to_check}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching insider data"):
            result = future.result()
            if result is not None:
                all_insider_data.append(result)

    if not all_insider_data:
        print("No insider transactions found for the specified scope.")
        return pd.DataFrame()

    master_df = pd.concat(all_insider_data, ignore_index=True)

    master_df.rename(columns={
        'Insider': 'insider_name',
        'Position': 'insider_position',
        'Start Date': 'transaction_date',
        'Transaction': 'transaction_type',
        'Shares': 'shares',
        'Value': 'value'
    }, inplace=True)

    print(f"Successfully fetched {len(master_df)} total insider transaction records.")
    return master_df


if __name__ == "__main__":
    data = fetch(scope='top_250_sp500', max_workers=20)
