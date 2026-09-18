"""
pandas_tests.py

Provides functions for running and diplaying the Pandas
performance tests.
"""

import pandas as pd

from src.benchmark import Benchmark
from src.pandas_runner import PandasRunner
from src.utils import print_section, print_memory_usage




def run_pandas_benchmark(
        runner: PandasRunner,
        benchmark: Benchmark,
) -> None:
    """
    Run all current Pandas benchmark workloads.
    
    The Pandas DataFrames are loaded once and then reused for
    the in-memory analytical workloads.
    """

    # Remove the old results before appending new ones.
    benchmark.remove_results_for_engine(
        Benchmark.ENGINE_PANDAS
    )

    print_section("PANDAS PERFORMANCE TEST")

    print("\nLoading CSV with Pandas...")
    csv_dataframe, csv_load_time = runner.load_csv()

    print(f"CSV load time: {csv_load_time:.3f} seconds")
    print(f"CSV rows:      {len(csv_dataframe):,}")
    print(f"CSV columns:   {len(csv_dataframe.columns)}")


    print("\nLoading Parquet with Pandas...")
    parquet_dataframe, parquet_load_time = runner.load_parquet()

    print(f"Parquet load time: {parquet_load_time:.3f} seconds")
    print(f"Parquet rows:      {len(parquet_dataframe):,}")
    print(f"Parquet columns:   {len(parquet_dataframe.columns)}")

    print_section("PANDAS FULL READ RESULTS")
    print(f"CSV:     {csv_load_time:.3f} seconds")
    print(f"Parquet: {parquet_load_time:.3f} seconds")

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FULL_READ,
        elapsed_time=csv_load_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FULL_READ,
        elapsed_time=parquet_load_time,
    )


    run_pandas_filter_and_aggregation(
        runner=runner,
        benchmark=benchmark,
        csv_dataframe=csv_dataframe,
        parquet_dataframe=parquet_dataframe,
    )

    run_pandas_group_by_aggregation(
        runner=runner,
        benchmark=benchmark,
        csv_dataframe=csv_dataframe,
        parquet_dataframe=parquet_dataframe,
    )

    run_pandas_column_projection(
            runner=runner,
            benchmark=benchmark,
            csv_dataframe=csv_dataframe,
            parquet_dataframe=parquet_dataframe,
        )

    run_pandas_direct_projection_load(
            runner=runner,
            benchmark=benchmark,
        )

    run_pandas_filter_and_projection(
            runner=runner,
            benchmark=benchmark,
            csv_dataframe=csv_dataframe,
            parquet_dataframe=parquet_dataframe,
        )


def run_pandas_filter_and_aggregation(
        runner: PandasRunner,
        benchmark: Benchmark,
        csv_dataframe: pd.DataFrame,
        parquet_dataframe: pd.DataFrame,
) -> None:
    """
    Run and validate the Pandas filter_and_aggregation workload.
    """

    print_section("FILTER AND AGGREGATION")
    print("\nFiltering CSV DataFrame...")

    (
        csv_fraud_count,
        csv_total_fraud,
        csv_analysis_time
    ) = runner.filter_and_aggregation(csv_dataframe)

    print(f"Fraud transactions: {csv_fraud_count:,}")
    print(f"Total fraud amount: {csv_total_fraud:,.2f}")
    print(f"Analysis time:      {csv_analysis_time:.3f} seconds")

    print("\nFiltering Parquet DataFrame...")

    (
        parquet_fraud_count,
        parquet_total_fraud,
        parquet_analysis_time
    ) = runner.filter_and_aggregation(parquet_dataframe)

    print(f"Fraud transactions: {parquet_fraud_count:,}")
    print(f"Total fraud amount: {parquet_total_fraud:,.2f}")
    print(f"Analysis time:      {parquet_analysis_time:.3f} seconds")

    print_section("ANALYTICAL VALIDATION")

    print(f"CSV fraud transactions:       {csv_fraud_count:,}")
    print(f"Parquet fraud transactions:   {parquet_fraud_count:,}")
    print(f"\nCSV total fraud amount:     {csv_total_fraud:,.2f}")
    print(f"\nParquet total fraud amount: {parquet_total_fraud:,.2f}")

    if (
        csv_fraud_count == parquet_fraud_count
        and csv_total_fraud == parquet_total_fraud
    ):
        print("\nStatus: PASS")
        print("Both formats produced the same analytical result.")
    else:
        print("\nStatus: FAIL")
        print("The analytical results do not match.")

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FILTER_AND_AGGREGATION,
        elapsed_time=csv_analysis_time,
    )
    
    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FILTER_AND_AGGREGATION,
        elapsed_time=parquet_analysis_time,
    )



def run_pandas_group_by_aggregation(
    runner: PandasRunner,
    benchmark: Benchmark,
    csv_dataframe: pd.DataFrame,
    parquet_dataframe: pd.DataFrame,
) -> None:
    """
    Run and validate the Pandas group-by aggregation workload.
    """

    print_section("GROUP BY AGGREGATION")
    print("\nAnalysing CSV DataFrame...")

    (
        csv_results,
        csv_analysis_time,
    ) = runner.group_by_aggregation(csv_dataframe)



    print(f"CSV time: {csv_analysis_time:.3f} seconds")
    print("CSV results:")
    print(csv_results.to_string(index=False))

    print("\nAnalysing Parquet DataFrame...")

    (
        parquet_results,
        parquet_analysis_time,
    ) = runner.group_by_aggregation(
        parquet_dataframe
    )

    print(f"Parquet time: {parquet_analysis_time:.3f} seconds")
    print("\nParquet results:")
    print(parquet_results.to_string(index=False))

    if csv_results.equals(parquet_results):
        print("\nStatus: PASS")
        print("Both formats produced the same group-by result.")
    else:
        print("\nStatus: FAIL")
        print("The group-by results do not match.")

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_GROUP_BY_AGGREGATION,
        elapsed_time=csv_analysis_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_GROUP_BY_AGGREGATION,
        elapsed_time=parquet_analysis_time,
    )



def run_pandas_column_projection(
        runner: PandasRunner,
        benchmark: Benchmark,
        csv_dataframe: pd.DataFrame,
        parquet_dataframe: pd.DataFrame,
) -> None:
    """
    Run and validate the in-memory column projection workload.
    """

    print_section("COLUMN PROJECTION WORKLOAD")

    print("\nSelecting columns from CSV DataFrame...")

    (
        csv_selected_columns,
        csv_projection_time,
    ) = runner.column_projection(csv_dataframe)

    print(f"CSV projection time: {csv_projection_time:.3f} seconds")
    print(f"Result columns: {list(csv_selected_columns.columns)}")

    print("\nSelecting columns from Parquet DataFrame...")

    (
        parquet_selected_columns,
        parquet_projection_time,
    ) = runner.column_projection(parquet_dataframe)

    print(f"Parquet projection time: {parquet_projection_time:.3f} seconds")
    print(f"Result columns: {list(parquet_selected_columns.columns)}")

    if csv_selected_columns.equals(parquet_selected_columns):
        print("\nStatus: PASS")
        print("Both formats produced the same projected columns.")
    else:
        print("\nStatus: FAIL")
        print("The projected columns do not match.")

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_COLUMN_PROJECTION,
        elapsed_time=csv_projection_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_COLUMN_PROJECTION,
        elapsed_time=parquet_projection_time,
    )



def run_pandas_direct_projection_load(
        runner: PandasRunner,
        benchmark: Benchmark,
) -> None:
    """
    Run the direct-from-disk column projection test.
    """

    print_section("COLUMN PROJECTION LOAD (DIRECT FROM DISK)")

    (
        selected_csv_dataframe,
        selected_parquet_dataframe,
        selected_csv_time,
        selected_parquet_time,
    ) = runner.load_selected_columns()

    benchmark.create_result(
        Benchmark.ENGINE_PANDAS,
        Benchmark.FORMAT_CSV,
        Benchmark.WORKLOAD_COLUMN_PROJECT_LOAD,
        selected_csv_time,
    )

    benchmark.create_result(
        Benchmark.ENGINE_PANDAS,
        Benchmark.FORMAT_PARQUET,
        Benchmark.WORKLOAD_COLUMN_PROJECT_LOAD,
        selected_parquet_time,
    )

    print("\nCSV:")
    print(f"  Load time: {selected_csv_time:.3f} seconds")
    print(f"  Rows:      {len(selected_csv_dataframe):,}")
    print(f"  Columns:   {list(selected_csv_dataframe.columns)}")

    print("\nParquet")
    print(f"  Load time: {selected_parquet_time:.3f} seconds")
    print(f"  Rows:      {len(selected_parquet_dataframe):,}")
    print(f"  Columns:   {list(selected_parquet_dataframe.columns)}")

    if selected_csv_dataframe.equals(selected_parquet_dataframe):
        print("\nStatus: PASS")
        print("Both formats produced the same direct-load result.")
    else:
        print("\nStatus: FAIL")
        print("The direct-load projection results do not match.")


def run_pandas_filter_and_projection(
        runner: PandasRunner,
        benchmark: Benchmark,
        csv_dataframe: pd.DataFrame,
        parquet_dataframe: pd.DataFrame,
) -> None:
    """
    Run and validate the  Pandas filter_and_projection workload.
    """

    print_section("FILTER AND PROJECT")

    print("\nFiltering and projecting CSV DataFrame...")

    csv_results, csv_time = runner.filter_and_projection(csv_dataframe)

    print(f"CSV time: {csv_time:.3f} seconds")
    print(f"CSV rows returned: {len(csv_results):,}")
    print(f"CSV columns: {list(csv_results.columns)}")

    print("\nFiltering and projecting Parquet DataFrame...")

    parquet_results, parquet_time = runner.filter_and_projection(parquet_dataframe)

    print(f"Parquet time: {parquet_time:.3f} seconds")
    print(f"Parquet rows returned: {len(parquet_results):,}")
    print(f"Parquet columns: {list(parquet_results.columns)}")

    if csv_results.equals(parquet_results):
        print("\nStatus: PASS")
        print("Both formats produced the same filter-and-project result.")
    else:
        print("\nStatus: FAIL")
        print("The filter-and-project results do not match.")

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FILTER_AND_PROJECTION,
        elapsed_time=csv_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PANDAS,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FILTER_AND_PROJECTION,
        elapsed_time=parquet_time,
    )

