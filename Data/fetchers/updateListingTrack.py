# scripts/fetchers/update_listing_track.py

import requests
import pandas as pd

def fetch():
    """
    Fetches all company listing events (IPOs, SPACs, etc.) from listingtrack.io.
    Returns a pandas DataFrame with 'symbol', 'name', and 'listing_method' columns.
    """
    print("Fetching listing events from ListingTrack API...")
    base_url = (
        "https://api.listingtrack.io/odata/companies"
        "?select=symbol,name,ipo"
        "&inclAll=true"
        "&$orderby=ipo/listingDate%20desc"
        "&$expand=ipo(select=listingMethod)"
    )

    all_records = []
    url = base_url
    page_num = 1

    while url:
        print(f"  - Fetching page {page_num}...")
        try:
            resp = requests.get(url)
            resp.raise_for_status()
            data = resp.json()
            
            batch = data.get('value')
            if not isinstance(batch, list):
                raise ValueError("Unexpected JSON structure")

            all_records.extend(batch)
            url = data.get('@odata.nextLink')
            page_num += 1
        except Exception as e:
            print(f"    Failed to fetch page {page_num}: {e}")
            url = None

    if not all_records:
        print("No records fetched from ListingTrack.")
        return pd.DataFrame()

    df = pd.json_normalize(all_records)
    print(f"Successfully fetched a total of {len(df)} listing event records.")
    
    df.rename(columns={'ipo.listingMethod': 'listing_method'}, inplace=True)
    df.dropna(subset=['symbol', 'name', 'listing_method'], inplace=True)
    
    return df[['symbol', 'name', 'listing_method']]

if __name__ == "__main__":
    data = fetch()
    if not data.empty:
        print("\n--- ListingTrack Data Sample ---")
        print(data.head())
        print("\n--- Listing Method Counts ---")
        print(data['listing_method'].value_counts())