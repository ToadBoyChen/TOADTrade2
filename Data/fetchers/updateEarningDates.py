import pandas as pd
import requests
import io
from datetime import date, timedelta
import time

def _scrape_earnings_for_day(day):
    """
    Internal helper to scrape the earnings calendar for a single day.
    """
    date_str = day.strftime("%Y-%m-%d")
    url = f"https://finance.yahoo.com/calendar/earnings?day={date_str}"
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    tables = pd.read_html(io.StringIO(response.text))
    if not tables:
        return pd.DataFrame()
        
    df = tables[0]
    df['earnings_date'] = pd.to_datetime(date_str).date()
    return df

def fetch():
    """
    Main fetch function. Scrapes earnings calendar for the next 90 days.
    Returns a DataFrame with 'symbol', 'name', 'earnings_date', 'eps_estimate', 'report_time'.
    """
    print("Fetching earnings calendar data from Yahoo Finance for the next 90 days...")
    
    all_earnings_dfs = []
    start_date = date.today()
    
    for i in range(90):
        current_date = start_date + timedelta(days=i)
        print(f"  - Scraping for date: {current_date.strftime('%Y-%m-%d')}...")
        try:
            day_df = _scrape_earnings_for_day(current_date)
            if not day_df.empty:
                all_earnings_dfs.append(day_df)
            time.sleep(0.1) 
        except Exception as e:
            if "No tables found" not in str(e):
                 print(f"    Could not scrape for {current_date}: {e}")

    if not all_earnings_dfs:
        print("Failed to scrape any earnings data.")
        return pd.DataFrame()
        
    master_df = pd.concat(all_earnings_dfs, ignore_index=True)
    
    master_df.rename(columns={
        'Symbol': 'symbol', 
        'Company': 'name',
        'Earnings Call Time': 'report_time',
        'EPS Estimate': 'eps_estimate'
    }, inplace=True)
    
    master_df.dropna(subset=['symbol', 'name'], inplace=True)
    if 'eps_estimate' not in master_df.columns:
        master_df['eps_estimate'] = None
    master_df['eps_estimate'] = pd.to_numeric(master_df['eps_estimate'], errors='coerce')
    
    print(f"Successfully scraped a total of {len(master_df)} earnings events.")
    
    return master_df[['symbol', 'name', 'earnings_date', 'eps_estimate', 'report_time']]

if __name__ == "__main__":
    data = fetch()
    if not data.empty:
        print("\n--- Earnings Data Sample ---")
        print(data.head())