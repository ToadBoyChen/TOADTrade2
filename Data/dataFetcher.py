import argparse
import DBManager
from fetchers import updateSP500
from fetchers import updateListingTrack 

FETCHER_MAPPING = {
    "sp500": {
        "module": updateSP500,
        "asset_class": "equity"
    },
    "listings": {
        "module": updateListingTrack,
        "asset_class": "equity"
    }
}

def run_fetch_and_store(fetcher_name):
    """
    Fetches data and stores it in the database.
    Now handles the multi-purpose listing fetcher.
    """
    if fetcher_name not in FETCHER_MAPPING:
        print(f"Error: Fetcher '{fetcher_name}' is not recognized. Skipping.")
        return

    config = FETCHER_MAPPING[fetcher_name]
    module = config["module"]
    asset_class = config["asset_class"]

    print(f"\n--- Processing fetcher: {fetcher_name} ---")
    
    assets_df = module.fetch()
    
    if assets_df.empty:
        print(f"--- No data returned from fetcher: {fetcher_name}. Skipping DB insert. ---")
        return

    if fetcher_name == 'listings':
        for method in assets_df['listing_method'].unique():
            print(f"  - Processing listing method: {method}")
            method_df = assets_df[assets_df['listing_method'] == method].copy()
            DBManager.upsert_assets(method_df, asset_class=asset_class, source=method.lower())
    else:
        DBManager.upsert_assets(assets_df, asset_class=asset_class, source=fetcher_name)

    print(f"--- Completed processing for: {fetcher_name} ---")

def main():
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