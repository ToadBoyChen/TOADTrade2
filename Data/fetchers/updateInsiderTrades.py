# Data/fetchers/updateInsiderTrades.py

import yfinance as yf
import pandas as pd
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
    Fetch recent insider transactions using yfinance.
    """
    print(f"Fetching insider transactions from yfinance with scope: '{scope}'")
    
    tickers_to_check = []
    if scope == 'top_10_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=10)
    elif scope == 'top_250_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=250)
    elif scope == 'sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=None)
    elif isinstance(scope, list):
        tickers_to_check = scope
    else:
        print(f"Error: Unknown scope '{scope}'. Aborting insider fetch.")
        return pd.DataFrame()

    print(f"Found {len(tickers_to_check)} tickers to check for insider trades.")
    
    all_insider_data = []

    for i, symbol in enumerate(tickers_to_check):
        print(f"  - Progress: [{i+1}/{len(tickers_to_check)}] Fetching trades for {symbol}...")
        try:
            ticker = yf.Ticker(symbol)
            transactions = ticker.insider_transactions
            
            if transactions is not None and not transactions.empty:
                transactions['symbol'] = symbol
                all_insider_data.append(transactions)


        except Exception as e:
            print(f"Could not fetch insider info for {symbol}: {e}")

    if not all_insider_data:
        print("No insider transactions found for the specified scope.")
        return pd.DataFrame()

    master_df = pd.concat(all_insider_data)
    
    master_df.reset_index(inplace=True)
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
    data = fetch(scope='top_10_sp500')
    if not data.empty:
        print("\n--- Insider Transactions Data Sample ---")
        print(data)