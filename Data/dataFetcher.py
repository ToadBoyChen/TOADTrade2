import argparse
import DBManager
from fetchers import updateSP500
from fetchers import updateListingTrack
from fetchers import updateMovers
from fetchers import updateEarningDates
from fetchers import updateAnalystRatings
from fetchers import updateInsiderTrades
from fetchers import updateDailyMetrics

FETCHER_MAPPING = {
    "sp500": {
        "module": updateSP500,
        "asset_class": "equity",
        "grouping_column": None 
    },
    "listings": {
        "module": updateListingTrack,
        "asset_class": "equity",
        "grouping_column": "listing_method" 
    },
    "anomalies": {
        "module": updateMovers,
        "asset_class": "equity",
        "grouping_column": "category"
    },
    # "earnings": {
    #     "module": updateEarningDates,
    #     "asset_class": "supplemental",
    #     "grouping_column": None 
    # },
    "analysis": {
            "module": updateAnalystRatings,
            "asset_class": "supplemental",
            "grouping_column": None,
            "fetch_args": {"scope": "top_10_sp500"}
    },
        "insiders": {
            "module": updateInsiderTrades,
            "asset_class": "supplemental",
            "grouping_column": None,
            "fetch_args": {"scope": "top_10_sp500"}
    },
        "daily_metrics": {
            "module": updateDailyMetrics,
            "asset_class": "metrics",
            "grouping_column": None,
            "fetch_args": {"scope": "top_10_sp500"}
    },
}

def run_fetch_and_store(fetcher_name):
    """
    Fetches data and stores it in the database.
    Generalized to handle simple, grouped, and special-case fetchers.
    """
    if fetcher_name not in FETCHER_MAPPING:
        print(f"Error: Fetcher '{fetcher_name}' is not recognized. Skipping.")
        return
    
    config = FETCHER_MAPPING[fetcher_name]
    module = config["module"]
    asset_class = config["asset_class"]
    grouping_col = config.get("grouping_column")
    fetch_args = config.get("fetch_args", {})

    print(f"\n--- Processing fetcher: {fetcher_name} ---")
    
    if fetch_args:
        print(f"  - Using fetch args: {fetch_args}")
        data_df = module.fetch(**fetch_args)
    else:
        data_df = module.fetch()
    
    if data_df.empty:
        print(f"--- No data returned from fetcher: {fetcher_name}. Skipping DB insert. ---")
        return

    if fetcher_name == 'earnings':
        print("  - Saving earnings calendar data...")

    elif fetcher_name == 'analysis':
        print("  - Saving analyst scores data...")
        DBManager.upsert_analyst_scores(data_df)
        
    elif fetcher_name == 'insiders':
        print("  - Saving insider transaction data...")
        DBManager.upsert_insider_transactions(data_df)
    
    elif fetcher_name == 'daily_metrics':
        print("  - Saving daily historical metrics...")
        DBManager.upsert_daily_metrics(data_df)

    elif grouping_col:
        for group_name in data_df[grouping_col].unique():
            print(f"  - Processing group: {group_name}")
            group_df = data_df[data_df[grouping_col] == group_name].copy()
            DBManager.upsert_assets(group_df, asset_class=asset_class, source=str(group_name).lower())
    else:
        DBManager.upsert_assets(data_df, asset_class=asset_class, source=fetcher_name)

    print(f"--- Completed processing for: {fetcher_name} ---")


def main():
    """
    Main function to parse arguments and orchestrate fetcher execution.
    """
    parser = argparse.ArgumentParser(description="TT2 Data Fetcher: A central hub for fetching data and storing it in the database.")
    
    parser.add_argument(
        "--fetch",
        nargs='+',
        choices=list(FETCHER_MAPPING.keys()),
        help=f"Specify one or more fetchers to run. Available: {', '.join(FETCHER_MAPPING.keys())}"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all available fetchers in sequence."
    )

    args = parser.parse_args()

    if args.all:
        fetchers_to_run = list(FETCHER_MAPPING.keys())
    elif args.fetch:
        fetchers_to_run = args.fetch
    else:
        parser.print_help()
        return

    for fetcher_name in fetchers_to_run:
        run_fetch_and_store(fetcher_name)
    
    print("\nAll data fetching tasks are complete.")

if __name__ == "__main__":
    main()