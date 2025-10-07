import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'tt2_data.db')

def create_database():
    """Creates the SQLite database and all necessary tables if they don't exist."""
    print(f"Initializing database at: {DB_PATH}")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        print("Creating 'assets' table...")
        create_assets_table_query = """
        CREATE TABLE IF NOT EXISTS assets (
            asset_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE NOT NULL,
            name TEXT,
            asset_class TEXT NOT NULL,
            source TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen TIMESTAMP,
            is_active BOOLEAN DEFAULT 1
        );
        """
        cursor.execute(create_assets_table_query)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON assets (symbol);")

        print("Creating 'daily_prices' table...")
        create_prices_table_query = """
        CREATE TABLE IF NOT EXISTS daily_prices (
            asset_id INTEGER NOT NULL,
            date DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            adj_close REAL NOT NULL,
            volume INTEGER NOT NULL,
            PRIMARY KEY (asset_id, date),
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id) ON DELETE CASCADE
        );
        """
        cursor.execute(create_prices_table_query)
        
        print("Creating 'earnings_calendar' table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS earnings_calendar (
            asset_id INTEGER NOT NULL,
            earnings_date DATE NOT NULL,
            eps_estimate REAL,
            report_time TEXT,
            PRIMARY KEY (asset_id, earnings_date),
            FOREIGN KEY (asset_id) REFERENCES assets (asset_id) ON DELETE CASCADE
        );
        """)
        
        print("Database and all tables initialized successfully.")

if __name__ == "__main__":
    create_database()