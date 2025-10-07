import pandas as pd
import requests

def fetch():
    """
    Scrapes the list of S&P 500 tickers from Wikipedia.
    Returns a pandas DataFrame with 'symbol' and 'name' columns.
    """
    print("Scraping S&P 500 data from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        sp500_table = tables[0]
        
        sp500_table['Symbol'] = sp500_table['Symbol'].str.replace('.', '-', regex=False)
        df = sp500_table[['Symbol', 'Security']].rename(columns={'Symbol': 'symbol', 'Security': 'name'})
        
        print(f"Successfully scraped {len(df)} tickers.")
        return df
        
    except Exception as e:
        print(f"Error scraping S&P 500 tickers: {e}")
        return pd.DataFrame()
    
if __name__ == "__main__":
    data = fetch()
    if not data.empty:
        print("\n--- Scraped Data Sample ---")
        print(data.head())