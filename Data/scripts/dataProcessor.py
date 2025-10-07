# scripts/dataProcessor.py

import argparse
import subprocess
import sys
import os

FETCHER_MAPPING = {
    "sp500": "update_sp500_tickers.py",
    "ipos": "update_ipo_calendar.py",
    "spacs": "update_spac_list.py",
}

def get_script_path(script_name):
    """Constructs the full, absolute path to a fetcher script."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "fetchers", script_name)

def run_fetcher(fetcher_name):
    """
    Executes a single fetcher script using a subprocess.
    """
    if fetcher_name not in FETCHER_MAPPING:
        print(f"Error: Fetcher '{fetcher_name}' is not recognized. Skipping.")
        return

    script_filename = FETCHER_MAPPING[fetcher_name]
    script_path = get_script_path(script_filename)

    print(f"--- Running fetcher: {fetcher_name} ({script_filename}) ---")

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        print(f"--- Successfully completed fetcher: {fetcher_name} ---\n")

    except FileNotFoundError:
        print(f"Error: Script not found at '{script_path}'. Check the FETCHER_MAPPING.")
    except subprocess.CalledProcessError as e:
        print(f"!!! Error executing fetcher: {fetcher_name} !!!")
        print(f"Return Code: {e.returncode}")
        print("--- STDOUT ---")
        print(e.stdout)
        print("--- STDERR ---")
        print(e.stderr)
        print(f"!!! Fetcher {fetcher_name} failed. Halting further processing could be an option here. !!!\n")
    except Exception as e:
        print(f"An unexpected error occurred while running {fetcher_name}: {e}\n")


def main():
    """
    Main function to parse arguments and orchestrate fetcher execution.
    """
    parser = argparse.ArgumentParser(description="TT2 Data Processor: A central hub for running data fetchers.")
    
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
        print("Running all available fetchers...")
        fetchers_to_run = list(FETCHER_MAPPING.keys())
    elif args.fetch:
        fetchers_to_run = args.fetch
    else:
        print("No fetchers specified. Use --fetch <name> or --all.")
        parser.print_help()
        return

    for fetcher_name in fetchers_to_run:
        run_fetcher(fetcher_name)
    
    print("All requested data processing tasks are complete.")


if __name__ == "__main__":
    main()