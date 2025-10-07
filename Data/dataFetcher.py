import argparse
import DBManager

from fetchers import updateSP500
from fetchers import updateIPO
from fetchers import updateSPAC

FETCHER_MAPPING = {
    "sp500": {
        "module": updateSP500,
        "asset_class": "equity"
    },
    "ipos": {
        "module": updateIPO,
        "asset_class": "equity"
    },
    "spacs": {
        "module": updateSPAC,
        "asset_class": "equity"
    },
}

def run_fetch_and_store(fetcher_name):
    """
    1. Gets the correct fetcher module from the mapping.
    2. Calls its `fetch()` function to get a DataFrame.
    3. Passes the DataFrame to the db_manager to be saved.
    """
    if fetcher_name not in FETCHER_MAPPING:
        print(f"Error: Fetcher '{fetcher_name}' is not recognized. Skipping.")
        return

    config = FETCHER_MAPPING[fetcher_name]
    module = config["module"]
    asset_class = config["asset_class"]

    print(f"\n--- Processing fetcher: {fetcher_name} ---")
    assets_df = module.fetch()
    DBManager.upsert_assets(assets_df, asset_class=asset_class, source=fetcher_name)
    
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