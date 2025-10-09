# scripts/fetchers/updateAnalystScores.py

import yfinance as yf
import pandas as pd
import sqlite3
import os
from datetime import date
from concurrent.futures import ThreadPoolExecutor
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

def fetch_single_score(symbol):
    """
    Fetches the analyst score for a single stock symbol.
    This function is designed to be run in a separate thread.
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.get_info()
        
        if 'recommendationMean' not in info and 'targetMeanPrice' not in info:
             return None

        return {
            'symbol': symbol,
            'name': info.get('shortName', symbol),
            'fetch_date': date.today(),
            'recommendation_mean': info.get('recommendationMean'),
            'recommendation_key': info.get('recommendationKey'),
            'analyst_count': info.get('numberOfAnalystOpinions'),
            'target_mean_price': info.get('targetMeanPrice'),
            'average_rating': info.get('averageAnalystRating')
        }
    except Exception as e:
        return None

def fetch(scope, max_workers=20):
    """
    Fetch current analyst ratings in parallel using a thread pool.
    """
    print(f"Fetching current analyst scores from yfinance with scope: '{scope}'")
    
    tickers_to_check = []
    if scope == 'top_10_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=10)
    elif scope == 'top_250_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=250)
    elif scope == 'sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500')
    elif isinstance(scope, list):
        tickers_to_check = scope
    else:
        print(f"Error: Unknown scope '{scope}'. Aborting scores fetch.")
        return pd.DataFrame()

    print(f"Found {len(tickers_to_check)} tickers to check for scores. Starting parallel fetch...")
    
    all_scores_data = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(fetch_single_score, tickers_to_check), total=len(tickers_to_check)))
    all_scores_data = [score for score in results if score is not None]

    if not all_scores_data:
        print("Failed to fetch any analyst scores for the specified scope.")
        return pd.DataFrame()

    master_df = pd.DataFrame(all_scores_data)
    print(f"Successfully fetched analyst scores for {len(master_df)} out of {len(tickers_to_check)} assets.")
    return master_df

if __name__ == "__main__":
    data = fetch('top_10_sp500') 
    if not data.empty:
        print("\n--- Analyst Scores Data Sample ---")
        print(data.head())