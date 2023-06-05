import argparse
from typing import List

import pandas as pd

def ensure_company_id_exists(columns: List[str]) -> None:
    assert "CompanyID" in columns, (
        "No CompanyID file exists in one of the provided files"
    )

def merge_csvs_on_company_id(
        first_dataframe: pd.DataFrame,
        second_dataframe: pd.DataFrame
) -> pd.DataFrame:
    try:
        combined_dataframes = first_dataframe.merge(
            second_dataframe,
            how="outer",
            left_on="CompanyID",
            right_on="CompanyID"
        )
    except Exception:
        print(
            "Error when attempting to combine CSV files using Pandas merge"
        )
        raise

    return combined_dataframes

def write_combined_data_to_csv(dataframe: pd.DataFrame, outfile: str) -> None:
    dataframe.to_csv(outfile, index=False)
    print(f"Output file containing combined CSVs has been written to: `{outfile}`")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file1",
        required=True,
        help="Filepath of the first file to be combined"
    )
    parser.add_argument(
        "--file2",
        required=True,
        help="Filepath for the second file to be combined"
    )
    parser.add_argument(
        "--outfile",
        required=False,
        help="Filepath to place the file containing the combined data."
    )
    args = parser.parse_args()

    first_file = pd.read_csv(args.file1)
    second_file = pd.read_csv(args.file2)
    for file in (first_file, second_file):
        ensure_company_id_exists(columns=file.columns)

    combined_dataframes = merge_csvs_on_company_id(
        first_dataframe=first_file,
        second_dataframe=second_file
    )
    print(
        f"The number of unique companies between all combined files is: "
        f"`{combined_dataframes['CompanyName'].nunique()}`"
    )
    write_combined_data_to_csv(
        dataframe=combined_dataframes,
        outfile="combined.csv" if args.outfile is None else args.outfile
    )

if __name__ == "__main__":
    main()