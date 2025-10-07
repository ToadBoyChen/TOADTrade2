# scripts/fetchers/update_spac_list.py

import pandas as pd

def fetch():
    """
    Generates a DUMMY list of active SPACs.
    Returns a pandas DataFrame with 'symbol' and 'name' columns.
    """
    print("Generating DUMMY SPAC data for testing...")
    
    # Create a list of dictionaries representing fake SPAC data
    dummy_spac_data = [
        {'symbol': 'PACB', 'name': 'Pioneer Acquisition Corp II'},
        {'symbol': 'GHV',  'name': 'Global Horizon Ventures'}
    ]
    
    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(dummy_spac_data)
    
    print(f"Generated {len(df)} dummy SPAC records.")
    return df

# This block allows you to test the script by itself if needed
if __name__ == "__main__":
    data = fetch()
    print("\n--- Dummy SPAC Data Sample ---")
    print(data.head())