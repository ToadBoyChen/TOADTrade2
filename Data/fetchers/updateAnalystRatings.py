# scripts/fetchers/updateAnalystScores.py

import yfinance as yf
import pandas as pd
import sqlite3
import os
from datetime import date

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'tt2_data.db')

def _get_tickers_from_db(source='sp500', limit=None):
    with sqlite3.connect(DB_PATH) as conn:
        query = f"SELECT symbol FROM assets WHERE is_active = 1 AND source = '{source}'"
        if limit:
            query += f" LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
    return df['symbol'].tolist()


def fetch(scope):
    """
    Fetch current analyst ratings and target prices using yfinance.
    """
    print(f"Fetching current analyst scores from yfinance with scope: '{scope}'")
    
    tickers_to_check = []
    if scope == 'top_10_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=10)
    elif scope == 'top_250_sp500':
        tickers_to_check = _get_tickers_from_db(source='sp500', limit=250)
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
            info = ticker.get_info()
            
            rec_mean = info.get('recommendationMean')
            avg_analyst_rating = info.get('averageAnalystRating')
            rec_key = info.get('recommendationKey')
            num_analysts = info.get('numberOfAnalystOpinions')
            tgt_mean_price = info.get('targetMeanPrice')
            
            score_data = {
                'symbol': symbol,
                'name': info.get('shortName', symbol),
                'fetch_date': date.today(),
                'recommendation_mean': rec_mean,
                'recommendation_key': rec_key,
                'analyst_count': num_analysts,
                'target_mean_price': tgt_mean_price,
                'average_rating': avg_analyst_rating
            }

            all_scores_data.append(score_data)

        except Exception as e:
            print(f"Could not fetch info for {symbol}: {e}")

    if not all_scores_data:
        print("Failed to fetch any analyst scores for the specified scope.")
        return pd.DataFrame()

    master_df = pd.DataFrame(all_scores_data)
    print(f"Successfully fetched analyst scores for {len(master_df)} assets.")
    return master_df


if __name__ == "__main__":
    data = fetch('top_250_sp500')
    if not data.empty:
        print("\n--- Analyst Scores Data Sample ---")
        print(data)
