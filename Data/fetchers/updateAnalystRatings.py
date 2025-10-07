# scripts/fetchers/updateAnalystScores.py

import yfinance as yf
import pandas as pd
import sqlite3
import os
import time
from datetime import date

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'tt2_data.db')

def _get_tickers_from_db(source='sp500', limit=None):
    with sqlite3.connect(DB_PATH) as conn:
        query = f"SELECT symbol FROM assets WHERE is_active = 1 AND source = '{source}'"
        if limit:
            query += f" LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
    return df['symbol'].tolist()

def fetch(scope='top_10_sp500'):
    """
    Main fetch function. Fetches current analyst scores for a specified scope of stocks.
    """
    print(f"Fetching current analyst scores from yfinance with scope: '{scope}'")
    
    tickers_to_check = []
    if scope == 'top_250_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=250)
    elif scope == 'sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=None)
    elif isinstance(scope, list):
        tickers_to_check = scope
    else:
        print(f"Error: Unknown scope '{scope}'. Aborting scores fetch.")
        return pd.DataFrame()

    print(f"Found {len(tickers_to_check)} tickers to check for scores.")
    
    all_scores_data = []

    for i, symbol in enumerate(tickers_to_check):
        print(f"  - Progress: [{i+1}/{len(tickers_to_check)}] Fetching score for {symbol}...")
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Extract the relevant data points from the .info dictionary
            # Use .get() to safely access keys that might not exist for some tickers
            score_data = {
                'symbol': symbol,
                'name': info.get('shortName', symbol), # Use shortName if available
                'fetch_date': date.today(),
                'recommendation_mean': info.get('recommendationMean'),
                'recommendation_key': info.get('recommendationKey'),
                'analyst_count': info.get('numberOfAnalystOpinions'),
                'target_mean_price': info.get('targetMeanPrice')
            }
            all_scores_data.append(score_data)

            time.sleep(0.1)

        except Exception as e:
            print(f"    Could not fetch info for {symbol}. Error: {e}")

    if not all_scores_data:
        print("Failed to fetch any analyst scores for the specified scope.")
        return pd.DataFrame()
        
    master_df = pd.DataFrame(all_scores_data)
    
    print(f"Successfully fetched analyst scores for {len(master_df)} assets.")
    
    return master_df

if __name__ == "__main__":
    data = fetch(scope='top_10_sp500')
    if not data.empty:
        print("\n--- Analyst Scores Data Sample ---")
        print(data)