import pandas as pd
import requests
import io

def _scrape_yahoo_screener(url):
    """
    Internal helper function to scrape a single Yahoo Finance screener page.
    Returns a pandas DataFrame.
    """
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    tables = pd.read_html(io.StringIO(response.text))
    if not tables:
        return pd.DataFrame()
        
    return tables[0]

def fetch():
    """
    Main fetch function. Scrapes Top Gainers, Losers, and Most Active stocks.
    Returns a single DataFrame with 'symbol', 'name', and 'category' columns.
    """
    print("Fetching market movers data from Yahoo Finance...")
    screener_urls = {
        'gainer': 'https://finance.yahoo.com/gainers',
        'loser': 'https://finance.yahoo.com/losers',
        'active': 'https://finance.yahoo.com/most-active',
        '52_week_high': 'https://finance.yahoo.com/research-hub/screener/recent_52_week_highs/',
        '52_week_low': 'https://finance.yahoo.com/research-hub/screener/recent_52_week_lows/'
    }
    
    all_movers_dfs = []

    for category, url in screener_urls.items():
        print(f"  - Scraping category: {category}...")
        try:
            df = _scrape_yahoo_screener(url)
            if df.empty:
                print(f"    No data found for {category}.")
                continue
            
            df['category'] = category
            all_movers_dfs.append(df)
            
        except Exception as e:
            print(f"    Failed to scrape {category}: {e}")

    if not all_movers_dfs:
        print("Failed to scrape any market mover data.")
        return pd.DataFrame()
    master_df = pd.concat(all_movers_dfs, ignore_index=True)
    master_df.rename(columns={'Symbol': 'symbol', 'Name': 'name'}, inplace=True)
    master_df.dropna(subset=['symbol', 'name'], inplace=True)
    
    print(f"Successfully scraped a total of {len(master_df)} market mover records.")
    return master_df[['symbol', 'name', 'category']]


if __name__ == "__main__":
    data = fetch()
    if not data.empty:
        print("\n--- Market Movers Data Sample ---")
        print(data.head())
        print("\n--- Category Counts ---")
        print(data['category'].value_counts())