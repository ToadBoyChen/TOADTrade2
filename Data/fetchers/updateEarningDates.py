import pandas as pd
import requests
import io
from datetime import date, timedelta
import time

# --- METHOD 1: Fast but sometimes fails for future dates ---
def _scrape_earnings_for_range(start_date, end_date):
    """Scrapes the earnings calendar for a given date range."""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    url = f"https://finance.yahoo.com/calendar/earnings?from={start_str}&to={end_str}&day={start_str}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(io.StringIO(response.text))
        return tables[0] if tables else pd.DataFrame()
    except Exception as e:
        print(f"    - Error during range scrape for {start_str} to {end_str}: {e}")
        return pd.DataFrame()

# --- METHOD 2: Slow but more reliable ---
def _scrape_earnings_for_day(day):
    """Scrapes the earnings calendar for a single day."""
    date_str = day.strftime("%Y-%m-%d")
    url = f"https://finance.yahoo.com/calendar/earnings?day={date_str}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(io.StringIO(response.text))
        if not tables:
            return pd.DataFrame()
        df = tables[0]
        # Manually add the earnings_date column, as this view doesn't have it
        df['Earnings Date'] = pd.to_datetime(date_str).date()
        return df
    except Exception:
        return pd.DataFrame()

def fetch():
    """
    Main fetch function using a hybrid approach.
    Tries to fetch weekly chunks, falls back to daily scraping if a chunk is malformed.
    """
    print("Fetching earnings calendar data from Yahoo Finance using hybrid approach...")
    
    all_earnings_dfs = []
    today = date.today()
    total_days_to_fetch = 91
    chunk_size_days = 7

    for i in range(0, total_days_to_fetch, chunk_size_days):
        start_of_chunk = today + timedelta(days=i)
        end_of_chunk = start_of_chunk + timedelta(days=chunk_size_days - 1)
        
        print(f"  - Scraping for date range: {start_of_chunk} to {end_of_chunk}...")
        
        # --- ATTEMPT 1: FAST METHOD ---
        week_df = _scrape_earnings_for_range(start_of_chunk, end_of_chunk)
        
        if not week_df.empty and 'Earnings Date' in week_df.columns:
            print("    -> Range scrape successful.")
            all_earnings_dfs.append(week_df)
        else:
            # --- ATTEMPT 2: SLOW FALLBACK METHOD ---
            print(f"    -> Range scrape failed or malformed. Falling back to daily scraping for this week.")
            for day_num in range(chunk_size_days):
                current_day = start_of_chunk + timedelta(days=day_num)
                day_df = _scrape_earnings_for_day(current_day)
                if not day_df.empty:
                    all_earnings_dfs.append(day_df)
                time.sleep(0.1) # Small sleep between daily requests

        time.sleep(0.5)

    if not all_earnings_dfs:
        print("Failed to scrape any earnings data.")
        return pd.DataFrame()
        
    master_df = pd.concat(all_earnings_dfs, ignore_index=True)
    
    # --- FINAL DATA CLEANING ---
    master_df.rename(columns={
        'Symbol': 'symbol', 
        'Company': 'name',
        'Earnings Call Time': 'report_time',
        'EPS Estimate': 'eps_estimate',
        'Earnings Date': 'earnings_datetime_str'
    }, inplace=True)

    datetimes = pd.to_datetime(master_df['earnings_datetime_str'], errors='coerce')
    master_df['earnings_date'] = datetimes.dt.date

    master_df.dropna(subset=['symbol', 'name', 'earnings_date'], inplace=True)
    
    if 'eps_estimate' not in master_df.columns:
        master_df['eps_estimate'] = None
    master_df['eps_estimate'] = pd.to_numeric(master_df['eps_estimate'], errors='coerce')

    master_df.drop_duplicates(subset=['symbol', 'earnings_date'], keep='first', inplace=True)
    
    print(f"\nSuccessfully scraped and processed a total of {len(master_df)} unique earnings events.")
    
    return master_df[['symbol', 'name', 'earnings_date', 'eps_estimate', 'report_time']]

if __name__ == "__main__":
    data = fetch()
    if not data.empty:
        print("\n--- Earnings Data Sample ---")
        print(data.head())