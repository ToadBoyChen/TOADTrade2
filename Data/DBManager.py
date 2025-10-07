# scripts/db_manager.py

import sqlite3
import os
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'tt2_data.db')


def upsert_earnings_calendar(earnings_df):
    """
    Inserts or updates data into the earnings_calendar table.
    """
    if not isinstance(earnings_df, pd.DataFrame) or earnings_df.empty:
        return

    print("Updating earnings calendar in the database...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            symbols = tuple(earnings_df['symbol'].unique())
            cursor.execute(f"SELECT symbol, asset_id FROM assets WHERE symbol IN ({','.join(['?']*len(symbols))})", symbols)
            symbol_to_id = dict(cursor.fetchall())
            
            earnings_df['asset_id'] = earnings_df['symbol'].map(symbol_to_id)
            earnings_df.dropna(subset=['asset_id'], inplace=True)
            earnings_df['asset_id'] = earnings_df['asset_id'].astype(int)
            
            records = earnings_df[['asset_id', 'earnings_date', 'eps_estimate', 'report_time']].to_records(index=False)
            
            upsert_query = """
            INSERT INTO earnings_calendar (asset_id, earnings_date, eps_estimate, report_time)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(asset_id, earnings_date) DO UPDATE SET
                eps_estimate=excluded.eps_estimate,
                report_time=excluded.report_time;
            """
            
            cursor.executemany(upsert_query, records)
            conn.commit()
            print(f"Successfully upserted {len(records)} records into 'earnings_calendar'.")
    except Exception as e:
        print(f"Database error while updating earnings calendar: {e}")

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
