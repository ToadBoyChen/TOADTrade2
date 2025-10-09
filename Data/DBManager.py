# scripts/db_manager.py

import sqlite3
import os
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'tt2_data.db')

def upsert_assets(assets_df, asset_class, source):
    """
    Inserts or updates asset data into the SQLite database.
    """
    if not isinstance(assets_df, pd.DataFrame) or assets_df.empty:
        print(f"Warning: Received empty data for source '{source}'. Skipping.")
        return

    assets_df['asset_class'] = asset_class
    assets_df['source'] = source
    assets_df['last_seen'] = datetime.utcnow().isoformat()
    if 'symbol' not in assets_df.columns or 'name' not in assets_df.columns:
        print(f"Error: DataFrame for source '{source}' is missing 'symbol' or 'name'.")
        return
        
    column_order = ['symbol', 'name', 'asset_class', 'source', 'last_seen']
    assets_df = assets_df[column_order]
    records_to_upsert = [tuple(x) for x in assets_df.to_records(index=False)]

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            upsert_query = """
            INSERT INTO assets (symbol, name, asset_class, source, last_seen)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name=excluded.name,
                asset_class=excluded.asset_class,
                source=excluded.source,
                last_seen=excluded.last_seen,
                is_active=1;
            """
            cursor.executemany(upsert_query, records_to_upsert)
            conn.commit()
            print(f"Successfully upserted {len(records_to_upsert)} records into 'assets' from source '{source}'.")
    except Exception as e:
        print(f"Database error for source '{source}': {e}")


def upsert_analyst_scores(scores_df):
    """
    Inserts or updates data into the analyst_scores table using the symbol as the key.
    """
    if not isinstance(scores_df, pd.DataFrame) or scores_df.empty:
        return

    print("Updating analyst scores in the database...")
    try:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            cols_to_insert = [
                'symbol', 
                'fetch_date', 'recommendation_mean', 'recommendation_key', 
                'analyst_count', 'target_mean_price', 'average_rating'
            ]
            records = scores_df[cols_to_insert].to_records(index=False)
            
            upsert_query = """
            INSERT INTO analyst_scores (
                asset_symbol, fetch_date, recommendation_mean, recommendation_key, 
                analyst_count, target_mean_price, average_rating
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(asset_symbol) DO UPDATE SET
                fetch_date=excluded.fetch_date,
                recommendation_mean=excluded.recommendation_mean,
                recommendation_key=excluded.recommendation_key,
                analyst_count=excluded.analyst_count,
                target_mean_price=excluded.target_mean_price,
                average_rating=excluded.average_rating,
                updated_at=datetime('now');
            """
            
            cursor = conn.cursor()
            cursor.executemany(upsert_query, records)
            conn.commit()
            print(f"Successfully upserted {len(records)} records into 'analyst_scores'.")
    except Exception as e:
        print(f"Database error while updating analyst scores: {e}")
        
        

def upsert_insider_transactions(transactions_df):
    """
    Inserts new insider transaction records into the database.
    """
    if not isinstance(transactions_df, pd.DataFrame) or transactions_df.empty:
        return

    print("Inserting insider transactions into the database...")
    try:
        transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date']).dt.strftime('%Y-%m-%d')
        
        cols_to_insert = [
            'symbol', 'insider_name', 'insider_position', 'transaction_date',
            'transaction_type', 'shares', 'value'
        ]
        records = transactions_df[cols_to_insert].to_records(index=False)
        
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO insider_transactions (
                asset_symbol, insider_name, insider_position, transaction_date,
                transaction_type, shares, value
            )
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """
            cursor.executemany(insert_query, records)
            conn.commit()
            print(f"Successfully inserted {len(records)} records into 'insider_transactions'.")

    except Exception as e:
        print(f"Database error while inserting insider transactions: {e}")