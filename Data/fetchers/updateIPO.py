# scripts/fetchers/update_ipo_calendar.py

import pandas as pd

def fetch():
    """
    Generates a DUMMY list of recent and upcoming IPOs.
    Returns a pandas DataFrame with 'symbol' and 'name' columns.
    """
    print("Generating DUMMY IPO data for testing...")
    
    # Create a list of dictionaries representing fake IPO data
    dummy_ipo_data = [
        {'symbol': 'INAI', 'name': 'Innovate AI Corp'},
        {'symbol': 'QLS',  'name': 'QuantumLeap Solutions'},
        {'symbol': 'BGF',  'name': 'BioGen Futures Inc.'}
    ]
    
    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(dummy_ipo_data)
    
    print(f"Generated {len(df)} dummy IPO records.")
    return df

# This block allows you to test the script by itself if needed
if __name__ == "__main__":
    data = fetch()
    print("\n--- Dummy IPO Data Sample ---")
    print(data.head())