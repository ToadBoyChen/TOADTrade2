import argparse
import DBManager
import shutil
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
    "earnings": {
         "module": updateEarningDates,
         "asset_class": "supplemental",
         "grouping_column": None,
         "fetch_args": {"scope": "sp500"},
    },
    "analysis": {
        "module": updateAnalystRatings,
        "asset_class": "supplemental",
        "grouping_column": None,
        "fetch_args": {"scope": "sp500"}
    },
    "insiders": {
        "module": updateInsiderTrades,
        "asset_class": "supplemental",
        "grouping_column": None,
        "fetch_args": {"scope": "sp500"}
    },
    "daily_metrics": {
        "module": updateDailyMetrics,
        "asset_class": "metrics",
        "grouping_column": None,
        "fetch_args": {"scope": "sp500"}
    },
}

def print_separator(char="="):
    width = shutil.get_terminal_size((80, 20)).columns
    print(char * width)

def run_fetch_and_store(fetcher_name):
    if fetcher_name not in FETCHER_MAPPING:
        print(f"Error: Fetcher '{fetcher_name}' is not recognized. Skipping.")
        return

    config = FETCHER_MAPPING[fetcher_name]
    module = config["module"]
    asset_class = config["asset_class"]
    grouping_col = config.get("grouping_column")
    fetch_args = config.get("fetch_args", {})

    print(f"\nProcessing fetcher: {fetcher_name}")
    print_separator()

    if fetch_args:
        print(f"  - Using fetch args: {fetch_args}")
        data_df = module.fetch(**fetch_args)
    else:
        data_df = module.fetch()

    if data_df.empty:
        print(f"No data returned from fetcher: {fetcher_name}. Skipping DB insert.")
        return

    if fetcher_name == 'analysis':
        print("  - Saving analyst scores data...")
        DBManager.upsert_analyst_scores(data_df)

    elif fetcher_name == 'insiders':
        print("  - Saving insider transaction data...")
        DBManager.upsert_insider_transactions(data_df)

    elif fetcher_name == 'daily_metrics':
        print("  - Saving daily historical metrics...")
        DBManager.upsert_daily_metrics(data_df)

    elif fetcher_name == 'earnings':
        print(" - saving earnings dates data...")
        DBManager.upsert_earnings_dates(data_df)

    elif grouping_col:
        for group_name in data_df[grouping_col].unique():
            print(f"  - Processing group: {group_name}...")
            group_df = data_df[data_df[grouping_col] == group_name].copy()
            DBManager.upsert_assets(
                group_df, asset_class=asset_class, source=str(group_name).lower()
            )
    else:
        DBManager.upsert_assets(data_df, asset_class=asset_class, source=fetcher_name)

    print_separator()
    print(f"Completed processing for: {fetcher_name}")
    print("")
    print("")


def show_menu():
    print("\nTT2 Data Fetcher Menu")
    print("===================================")
    print("1) Run ALL Fetchers")
    print("2) Fetch Listings / IPOs")
    print("3) Fetch Market Anomalies (Movers)")
    print("4) Fetch Analyst Ratings")
    print("5) Fetch Insider Trades")
    print("6) Fetch Daily Metrics")
    print("7) Fetch S&P500 Constituents")
    print("8) Fetch Earning Dates")
    print("0) Exit")
    print("===================================")

    choice = input("Enter your choice: ").strip()

    mapping = {
        "1": "all",
        "2": "listings",
        "3": "anomalies",
        "4": "analysis",
        "5": "insiders",
        "6": "daily_metrics",
        "7": "sp500",
        "8": "earnings"
    }

    if choice == "0":
        print("Exiting...")
        return []

    return [mapping.get(choice)] if choice in mapping and mapping[choice] != "all" else list(FETCHER_MAPPING.keys())


def main():
    parser = argparse.ArgumentParser(
        description="TT2 Data Fetcher: Fetches and stores financial datasets."
    )

    parser.add_argument(
        "--fetch",
        nargs="+",
        choices=list(FETCHER_MAPPING.keys()),
        help=f"Specify one or more fetchers to run. Available: {', '.join(FETCHER_MAPPING.keys())}"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all available fetchers."
    )

    args = parser.parse_args()

    if not args.fetch and not args.all:
        fetchers_to_run = show_menu()
        if not fetchers_to_run:
            return
    elif args.all:
        fetchers_to_run = list(FETCHER_MAPPING.keys())
    else:
        fetchers_to_run = args.fetch

    print("\nStarting data fetch sequence...\n")
    for fetcher_name in fetchers_to_run:
        run_fetch_and_store(fetcher_name)

    print("\nAll data fetching tasks are complete.")


if __name__ == "__main__":
    main()
