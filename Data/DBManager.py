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
        print(f"Warning: Received empty or invalid data for source '{source}'. Skipping DB operation.")
        return

    if 'symbol' not in assets_df.columns or 'name' not in assets_df.columns:
        print(f"Error: DataFrame for source '{source}' is missing 'symbol' or 'name' column.")
        return

    assets_df['asset_class'] = asset_class
    assets_df['source'] = source
    assets_df['last_seen'] = datetime.utcnow().isoformat()
    assets_df['is_active'] = 1
    
    column_order = ['symbol', 'name', 'asset_class', 'source', 'last_seen', 'is_active']
    assets_df = assets_df[column_order]
    
    records_to_upsert = [tuple(x) for x in assets_df.to_records(index=False)]

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            upsert_query = """
            INSERT INTO assets (symbol, name, asset_class, source, last_seen, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name=excluded.name,
                asset_class=excluded.asset_class,
                source=excluded.source,
                last_seen=excluded.last_seen,
                is_active=excluded.is_active;
            """
            
            cursor.executemany(upsert_query, records_to_upsert)
            conn.commit()
            print(f"Successfully upserted {len(records_to_upsert)} records into DB from source '{source}'.")
    except Exception as e:
        print(f"Database error for source '{source}': {e}")
