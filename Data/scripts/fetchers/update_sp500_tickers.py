# scripts/update_sp500_tickers.py

import pandas as pd
import yaml
import requests

def scrape_sp500_tickers():
    """
    Scrapes the list of S&P 500 tickers from the Wikipedia page.
    Returns a list of tickers.
    """
    print("Scraping S&P 500 tickers from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        
        sp500_table = tables[0]
        tickers = sp500_table['Symbol'].str.replace('.', '-', regex=False).tolist()
        
        print(f"Successfully scraped {len(tickers)} tickers.")
        return tickers
        
    except Exception as e:
        print(f"Error scraping S&P 500 tickers: {e}")
        return []

def save_tickers_to_config(tickers, config_path='./config.yaml'):
    """
    Loads the existing config, updates the yfinance tickers list,
    and saves it back.
    """
    if not tickers:
        print("No tickers to save. Aborting.")
        return
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        config['data_sources']['yfinance']['tickers'] = tickers
        
        with open(config_path, 'w') as file:
            yaml.dump(config, file, sort_keys=False)
            
        print(f"Successfully updated '{config_path}' with {len(tickers)} tickers.")
    except FileNotFoundError:
        print(f"Error: Could not find the config file at '{config_path}'")


if __name__ == "__main__":
    sp500_tickers = scrape_sp500_tickers()
    if sp500_tickers:
        save_tickers_to_config(sp500_tickers)
