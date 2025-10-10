import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'tt2_data.db')

def create_database():
    """Creates the SQLite database and all necessary tables if they don't exist."""
    print(f"Initializing database at: {DB_PATH}")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        print("Creating 'assets' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            symbol TEXT PRIMARY KEY NOT NULL,
            name TEXT,
            asset_class TEXT NOT NULL,
            source TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        );
        """)

        print("Creating 'earnings_calendar' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS earnings_calendar (
            asset_symbol TEXT NOT NULL,
            earnings_date DATE NOT NULL,
            eps_estimate REAL,
            report_time TEXT,
            PRIMARY KEY (asset_symbol, earnings_date),
            FOREIGN KEY (asset_symbol) REFERENCES assets (symbol) ON DELETE CASCADE
        );
        """)

        print("Creating 'analyst_scores' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS analyst_scores (
            asset_symbol TEXT PRIMARY KEY NOT NULL,
            fetch_date DATE NOT NULL,
            recommendation_mean REAL,
            recommendation_key TEXT,
            analyst_count REAL,
            target_mean_price REAL,
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (asset_symbol) REFERENCES assets (symbol) ON DELETE CASCADE
        );
        """)
        
        print("Creating 'insider_transactions' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS insider_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_symbol TEXT NOT NULL,
            insider_name TEXT,
            insider_position TEXT,
            transaction_date DATE,
            transaction_type TEXT,
            shares INTEGER,
            value REAL,
            fetch_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (asset_symbol) REFERENCES assets (symbol) ON DELETE CASCADE
        );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_insider_asset_symbol ON insider_transactions (asset_symbol);")
        
        print("Creating 'daily_metrics' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            asset_symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            volatility_30d REAL,
            ma_20d REAL,        
            ma_50d REAL,        
            rsi_14d REAL,       
            PRIMARY KEY (asset_symbol, date),
            FOREIGN KEY (asset_symbol) REFERENCES assets (symbol) ON DELETE CASCADE
        );
        """)

        conn.commit()
        print("Database and all tables initialized successfully.")

if __name__ == "__main__":
    create_database()
