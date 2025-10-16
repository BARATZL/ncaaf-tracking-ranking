from functions.fetch_rankings import fetch_rankings_from_espn
from functions.parse_rankings import parse_rankings_data
from functions.export_rankings import export_rankings_to_file

def main():
    print("🚀 Starting ESPN Ranking Pipeline")
    
    data = fetch_rankings_from_espn()
    poll_dfs = parse_rankings_data(data)
    export_rankings_to_file(poll_dfs)

    print("✅ Pipeline finished successfully!")

if __name__ == "__main__":
    main()
