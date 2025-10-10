import sys
import subprocess

def run_command(command):
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            text=True
        )
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        return False


def setup_database():
    print("\nSetting up the database...")
    run_command("rm tt2_data.db")
    database_setup = run_command("python Data/DBSetUp.py")
    if database_setup:
        print("Database setup complete.")
    else:
        print("Database setup encountered an error.")


def fetch_data_menu():
    data_yes_no_choice = input("\nWould you like new or more data (y/n)?")

    if data_yes_no_choice == "y":
        data_fetch_setup = run_command("python Data/dataFetcher.py")
        if data_fetch_setup:
            print("Data fetch complete.")
        else:
            print("Data fetch encountered an error.")
    elif data_yes_no_choice == "n":
        print()
    else:
        print("Invalid choice. Exiting Program.")
        sys.exit(0)

def main():
    print("💰 Welcome to TT2: Trading Toolkit 2.0 💰")
    print("========================================\n")

    setup = input("Would you like to set up the database? (y/n): ").strip().lower()
    if setup == "y":
        setup_database()
    else:
        print("Skipping database setup.")

    fetch_data_menu()


if __name__ == "__main__":
    main()
