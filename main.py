"""
main.py

Initial test for the Parquet Performance Analysis project.

This program:
1. Locates the CSV and Parquet datasets.
2. Reports their file sizes.
3. Loads both files with PySpark.
4. Displays their schemas.
5. Counts their rows.
6. Verifies that both datasets contain the same number of rows.
"""

from pathlib import Path

from pyspark.sql import SparkSession

from src.dataset_loader import DatasetLoader
from src.pandas_runner import PandasRunner



def main() -> None:
    """
    Run the initial dataset loading and validation test.
    """

    project_directory = Path(__file__).resolve().parent

    csv_path = project_directory / "data" / "AIML Dataset.csv"
    parquet_path = project_directory / "data" / "AIML Dataset.parquet"

    spark = (
        SparkSession.builder
        .appName("ParquetPerformanceAnalysis")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    loader = DatasetLoader(
        spark=spark,
        csv_path=csv_path,
        parquet_path=parquet_path,
    )

    print("=" * 60)
    print("PARQUET PERFORMNANCE ANALYSIS")
    print("=" * 60)

    print("\nDATASET FILES")
    print("-" * 60)

    print(f"CSV:")
    print(f"  Path: {csv_path}")
    print(f"  Size: {loader.get_csv_size_mb():,.2f} MB")

    print(f"Parquet:")
    print(f"  Path: {parquet_path}")
    print(f"  Size: {loader.get_parquet_size_mb():,.2f} MB")

    try:
        print("\n" + "=" * 60)
        print("LOADING CSV")
        print("=" * 60)

        csv_df = loader.load_csv()
        csv_df.printSchema()
        csv_row_count = csv_df.count()
        print(f"CSV row count: {csv_row_count:,}")

        print("\n" + "=" * 60)
        print("LOADING PARQUET")
        print("=" * 60)

        parquet_df = loader.load_parquet()

        parquet_df.printSchema()

        parquet_row_count = parquet_df.count()

        print(f"Parquet row count: {parquet_row_count:,}")

        print("\n" + "=" * 60)
        print("DATA VALIDATION")
        print("=" * 60)

        print(f"    CSV rows: {csv_row_count}")
        print(f"Parquet rows: {parquet_row_count}")

        if csv_row_count == parquet_row_count:
            print("\nStatus: PASS")
            print("Both files contain the same number of rows.")
        else:
            print("\nStatus: FAIL")
            print("The CSV and Parquet row counts do not match.")


    ############################################################
    ############################################################
    ############################################################

        print("\n" + "=" * 60)
        print("PANDAS PERFORMANCE TEST")
        print("=" * 60)

        pandas_runner = PandasRunner(
            csv_path=csv_path,
            parquet_path=parquet_path,
        )

        print("\nLoading CSV with Pandas...")

        pandas_csv_df, csv_load_time = pandas_runner.load_csv()

        print(f"CSV load time: {csv_load_time:.3f} seconds")
        print(f"CSV rows:      {len(pandas_csv_df):,}")
        print(f"CSV columns:   {len(pandas_csv_df.columns)}")

        print("\nLoading Parquet with Pandas...")

        pandas_parquet_df, parquet_load_time = pandas_runner.load_parquet()

        print(f"Parquet load time: {parquet_load_time:.3f} seconds")
        print(f"Parquet rows:      {len(pandas_parquet_df):,}")
        print(f"Parquet columns:   {len(pandas_parquet_df.columns)}")

        print("\n" + "=" * 60)
        print("PANDAS RESULTS")
        print("=" * 60)

        print(f"CSV:      {csv_load_time:.3f} seconds")
        print(f"Parquet:  {parquet_load_time:.3f} seconds")

    finally:
        spark.stop()


    ############################################################
    ############################################################
    ############################################################

    print("\n" + "=" * 60)
    print("PANDAS ANALYTICAL WORKLOAD")
    print("=" * 60)

    print("\nAnalysing CSV DataFrame...")

    (
        csv_fraud_count,
        csv_total_fraud,
        csv_analysis_time,
    ) = pandas_runner.analyse_fraud(pandas_csv_df)

    print(f"Fraud transactions: {csv_fraud_count:,}")
    print(f"Total fraud amount:  {csv_total_fraud:,.2f}")
    print(f"Analysis time:       {csv_analysis_time:.3f} seconds")

    print("\nAnalysing Parquet DataFrame...")

    (
        parquet_fraud_count,
        parquet_total_fraud,
        parquet_analysis_time,
    ) = pandas_runner.analyse_fraud(pandas_parquet_df)

    print(f"Fraud transactions: {parquet_fraud_count:,}")
    print(f"Total fraud amount:  {parquet_total_fraud:,.2f}")
    print(f"Analysis time:       {parquet_analysis_time:.3f} seconds")


    ############################################################
    ############################################################
    ############################################################

    print("\n" + "=" * 60)
    print("ANALYTICAL VALIDATION")
    print("=" * 60)

    print(f"CSV fraud transactions:      {csv_fraud_count:,}")
    print(f"Parquet fraud transactions:  {parquet_fraud_count:,}")

    print(f"\nCSV total fraud amount:      {csv_total_fraud:,.2f}")
    print(f"Parquet total fraud amount:  {parquet_total_fraud:,.2f}")

    if (
        csv_fraud_count == parquet_fraud_count
        and csv_total_fraud == parquet_total_fraud
    ):
        print("\nStatus: PASS")
        print("Both formats produced the same analytical result.")
    else:
        print("\nStatus: FAIL")
        print("The analytical results do not match.")


    ############################################################
    ############################################################
    ############################################################

    print("\n" + "=" * 60)
    print("TRANSACTION TYPE ANALYSIS")
    print("=" * 60)

    print("\nAnalysing CSV DataFrame...")

    (
        csv_type_results,
        csv_type_analysis_time,
    ) = pandas_runner.analyse_by_transaction_type(
        pandas_csv_df
    )

    print(f"Analysis time: {csv_type_analysis_time:.3f} seconds")
    print("\nCSV results:")
    print(csv_type_results.to_string(index=False))

    print("\nAnalysing Parquet DataFrame...")

    (
        parquet_type_results,
        parquet_type_analysis_time,
    ) = pandas_runner.analyse_by_transaction_type(
        pandas_parquet_df
    )

    print(f"Analysis time: {parquet_type_analysis_time:.3f} seconds")
    print("\nParquet results:")
    print(parquet_type_results.to_string(index=False))


    ############################################################
    ############################################################
    ############################################################

    print("\n" + "=" * 60)
    print("COLUMN PROJECTION WORKLOAD")
    print("=" * 60)

    print("\nSelecting columns from CSV DataFrame...")

    (
        csv_selected_columns,
        csv_projection_time,
    ) = pandas_runner.analyse_selected_columns(
        pandas_csv_df
    )

    print(f"CSV projection time: {csv_projection_time:.3f} seconds")
    print(f"Result columns: {list(csv_selected_columns.columns)}")

    print("\nSelecting columns from Parquet DataFrame...")

    (
        parquet_selected_columns,
        parquet_projection_time,
    ) = pandas_runner.analyse_selected_columns(
        pandas_parquet_df
    )

    print(
        f"Parquet projection time: "
        f"{parquet_projection_time:.3f} seconds"
    )

    print(
        f"Result columns: "
        f"{list(parquet_selected_columns.columns)}"
    )


    ############################################################
    ############################################################
    ############################################################

    print("\n" + "=" * 60)
    print("COLUMN PROJECTION LOAD (direct from disk)")
    print("=" * 60)

    (
        selected_csv_df,
        selected_parquet_df,
        selected_csv_time,
        selected_parquet_time,
    ) = pandas_runner.load_selected_columns()

    print("\nCSV:")
    print(f"  Load time: {selected_csv_time:.3f} seconds")
    print(f"  Rows:      {len(selected_csv_df):,}")
    print(f"  Columns:   {list(selected_csv_df.columns)}")

    print("\nParquet:")
    print(f"  Load time: {selected_parquet_time:.3f} seconds")
    print(f"  Rows:      {len(selected_parquet_df):,}")
    print(f"  Columns:   {list(selected_parquet_df.columns)}")



    
if __name__ == "__main__":
    main()

