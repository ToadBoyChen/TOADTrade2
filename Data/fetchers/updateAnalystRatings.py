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

        # --- Skip tickers without analyst data ---
        if not info:
            return None
        if (
            'recommendationMean' not in info and
            'targetMeanPrice' not in info and
            'numberOfAnalystOpinions' not in info and
            'analystCount' not in info
        ):
            return None

        # --- Flexible fallbacks for Yahoo's shifting key names ---
        analyst_count = (
            info.get('numberOfAnalystOpinions') or 
            info.get('analystCount') or 
            None
        )

        # --- Clean numeric types ---
        try:
            analyst_count = int(float(analyst_count)) if analyst_count is not None else None
        except (TypeError, ValueError):
            analyst_count = None

        recommendation_mean = info.get('recommendationMean')
        try:
            recommendation_mean = float(recommendation_mean) if recommendation_mean is not None else None
        except (TypeError, ValueError):
            recommendation_mean = None

        target_mean_price = info.get('targetMeanPrice')
        try:
            target_mean_price = float(target_mean_price) if target_mean_price is not None else None
        except (TypeError, ValueError):
            target_mean_price = None

        return {
            'symbol': symbol,
            'name': info.get('shortName', symbol),
            'fetch_date': date.today(),
            'recommendation_mean': recommendation_mean,
            'recommendation_key': info.get('recommendationKey'),
            'analyst_count': analyst_count,
            'target_mean_price': target_mean_price,
            'average_rating': info.get('averageAnalystRating')
        }
    except Exception as e:
        # Silently skip failed tickers to avoid halting multithreading
        return None

def fetch(scope, max_workers=20):
    """
    Fetch current analyst ratings in parallel using a thread pool.
    """
    print(f"Fetching current analyst scores from yfinance with scope: '{scope}'")
    
    # --- Determine tickers to check ---
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

    # --- Fetch in parallel ---
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(executor.map(fetch_single_score, tickers_to_check), total=len(tickers_to_check)))

    # --- Filter successful results ---
    all_scores_data = [score for score in results if score is not None]

    if not all_scores_data:
        print("⚠️ Failed to fetch any analyst scores for the specified scope.")
        return pd.DataFrame()

    master_df = pd.DataFrame(all_scores_data)

    # --- Ensure proper datatypes before passing to the DB manager ---
    master_df['fetch_date'] = pd.to_datetime(master_df['fetch_date']).dt.strftime('%Y-%m-%d')
    numeric_fields = ['recommendation_mean', 'analyst_count', 'target_mean_price']
    for col in numeric_fields:
        master_df[col] = pd.to_numeric(master_df[col], errors='coerce')

    print(f"✅ Successfully fetched analyst scores for {len(master_df)} out of {len(tickers_to_check)} assets.")
    return master_df


if __name__ == "__main__":
    data = fetch('top_10_sp500')
    if not data.empty:
        print("\n--- Analyst Scores Data Sample ---")
        print(data.head().to_string(index=False))