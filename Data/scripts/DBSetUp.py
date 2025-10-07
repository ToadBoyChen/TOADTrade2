import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'tt2_data.db')

def create_database():
    """Creates the SQLite database and the assets table if they don't exist."""
    print(f"Initializing database at: {DB_PATH}")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        create_table_query = """
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
        
        cursor.execute(create_table_query)
        
        create_index_query = "CREATE INDEX IF NOT EXISTS idx_symbol ON assets (symbol);"
        cursor.execute(create_index_query)

        print("Database and 'assets' table initialized successfully.")

if __name__ == "__main__":
    create_database()