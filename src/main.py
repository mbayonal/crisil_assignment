import argparse
from etl import PremierLeagueETL

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Premier League ETL")
    parser.add_argument("--input_path", required=True, help="Path to the input JSON files")
    parser.add_argument("--output_path", required=True, help="Path to store the output Parquet files")
    
    args = parser.parse_args()

    print(f"Running ETL with input path: {args.input_path} and output path: {args.output_path}")

    etl = PremierLeagueETL(input_path=args.input_path, output_path=args.output_path)
    etl.run()
