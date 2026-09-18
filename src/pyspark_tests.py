"""
pyspark_tests.py

Provides functions for running and diplaying the PySpark
performance tests.
"""

from pyspark.sql import DataFrame
from math import isclose

from src.benchmark import Benchmark
from src.pyspark_runner import PySparkRunner
from src.utils import print_section, values_are_equal





def run_pyspark_benchmark(
        runner: PySparkRunner,
        benchmark: Benchmark,
) -> None:
    """
    Run the initial PqSpark full-read benchmark.

    This currently measures only the complete parquet and Parquet reads.
    """

    # Remove the old results before appending new ones.
    benchmark.remove_results_for_engine(
        Benchmark.ENGINE_PYSPARK
    )

    print_section("PYSPARK FULL READ TEST")

    print("\nLoading CSV with PySpark...")

    csv_dataframe, csv_load_time = runner.load_csv()

    print(f"CSV load time: {csv_load_time:.3f} seconds")
    print(f"CSV rows:      {csv_dataframe.count():,}")

    print("\nLoading Parquet with PySpark...")

    parquet_dataframe, parquet_load_time = runner.load_parquet()

    print(f"Parquet load time: {parquet_load_time:.3f} seconds")
    print(f"Parquet rows:      {parquet_dataframe.count():,}")

    print_section("PySpark FULL READ RESULTS")
    print(f"CSV:     {csv_load_time:.3f} seconds")
    print(f"Parquet: {parquet_load_time:.3f} seconds")

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FULL_READ,
        elapsed_time=csv_load_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FULL_READ,
        elapsed_time=parquet_load_time,
    )


    run_pyspark_filter_and_aggregation(
        runner=runner,
        benchmark=benchmark,
        csv_dataframe=csv_dataframe,
        parquet_dataframe=parquet_dataframe,
    )

    run_pyspark_group_by_aggregation(
        runner=runner,
        benchmark=benchmark,
        csv_dataframe=csv_dataframe,
        parquet_dataframe=parquet_dataframe,
    )

    run_pyspark_column_projection(
        runner=runner,
        benchmark=benchmark,
        csv_dataframe=csv_dataframe,
        parquet_dataframe=parquet_dataframe,
    )

    run_pyspark_column_projection_load(
        runner=runner,
        benchmark=benchmark,
    )

    run_pyspark_filter_and_projection(
        runner=runner,
        benchmark=benchmark,
        csv_dataframe=csv_dataframe,
        parquet_dataframe=parquet_dataframe,
    )


def run_pyspark_filter_and_aggregation(
    runner: PySparkRunner,
    benchmark: Benchmark,
    csv_dataframe: DataFrame,
    parquet_dataframe: DataFrame,
) -> None:
    """
    Run the PySpark filter=and-aggregation workload.

    This counts fraudulent transactions and calculates the total amount
    associated with those transactions for both file formats.
    """

    print_section("PYSPARK FILTER AND AGGREGATION")

    print("\nTesting CSV...")

    (
        csv_fraud_count,
        csv_total_fraud_amount,
        csv_elapsed_time,
    ) = runner.filter_and_aggregation(csv_dataframe)

    print(f"CSV fraud count: {csv_fraud_count:,}")
    print(f"CSV total fraud amount: {csv_total_fraud_amount:,.2f}")
    print(f"CSV time: {csv_elapsed_time:.3f} seconds")

    print("\nTesting Parquet...")

    (
        parquet_fraud_count,
        parquet_total_fraud_amount,
        parquet_elapsed_time,
    ) = runner.filter_and_aggregation(parquet_dataframe)

    print(f"Parquet fraud count: {parquet_fraud_count:,}")
    print(f"Parquet total fraud amount: {parquet_total_fraud_amount:,.2f}")
    print(f"Parquet time: {parquet_elapsed_time:.3f} seconds")

    print_section("PYSPARK FILTER AND AGGREGATION RESULTS")

    print(f"CSV:     {csv_elapsed_time:.3f} seconds")
    print(f"Parquet: {parquet_elapsed_time:.3f} seconds")

    if (
        csv_fraud_count == parquet_fraud_count
        and values_are_equal(
            csv_total_fraud_amount,
            parquet_total_fraud_amount,
        )
    ):
        print("Validation: PASS")
        print("CSV and Parquet results are identical.")
    else:
        print("Validation: FAIL")
        print("CSV and Parquet results differ.")


    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FILTER_AND_AGGREGATION,
        elapsed_time=csv_elapsed_time,
    )    

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FILTER_AND_AGGREGATION,
        elapsed_time=parquet_elapsed_time,
    )


def run_pyspark_group_by_aggregation(
        runner: PySparkRunner,
        benchmark: Benchmark,
        csv_dataframe: DataFrame,
        parquet_dataframe: DataFrame
) -> None:
    """
    Run the PySpark group-by aggregation workload.

    Groups transactions by transaction type and calculateS:
    - transaction count
    - total transaction amount
    - average transaction amount
    - fraud count
    """

    print_section("PYSPARK GROUP BY AGGREGATION")    

    print("\nTesting CSV...")

    (
        csv_results,
        csv_elapsed_time,
    ) = runner.group_by_aggregation(csv_dataframe)



    print(f"CSV time: {csv_elapsed_time:.3f} seconds")
    print("CSV results:")
    csv_results.show()

    print("\nTesting Parquet...")

    (
        parquet_results,
        parquet_elapsed_time,
    ) = runner.group_by_aggregation(parquet_dataframe)

    print(f"Parquet time: {parquet_elapsed_time:.3f} seconds")
    print("Parquet results:")
    parquet_results.show()

    print_section("PYSPARK GROUP BY AGGREGATION RESULTS")

    print(f"CSV:     {csv_elapsed_time:.3f} seconds")
    print(f"Parquet: {parquet_elapsed_time:.3f} seconds")

    # There is no equals method for datasets in PySparks version of a DataFrame (unlike Pandas version), so we must .collect() the data again into a temp. list.
    csv_grouped = {
        row["type"]: row.asDict()
        for row in csv_results.collect()
    }

    parquet_grouped = {
        row["type"]: row.asDict()
        for row in parquet_results.collect()
    }

    results_match = True

    if csv_grouped.keys() != parquet_grouped.keys():
        results_match = False
    else:
        for transaction_type in csv_grouped:
            csv_row = csv_grouped[transaction_type]
            parquet_row = parquet_grouped[transaction_type]

            if (
                csv_row["transaction_count"]
                != parquet_row["transaction_count"]
                or csv_row["fraud_count"]
                != parquet_row["fraud_count"]
                or not values_are_equal(
                    csv_row["total_amount"],
                    parquet_row["total_amount"],
                )
                or not values_are_equal(
                    csv_row["average_amount"],
                    parquet_row["average_amount"],
                )
            ):
                results_match = False
                break

    if results_match:
        print("\nStatus: PASS")
        print("Both formats produced the same group-by result.")
    else:
        print("\nStatus: FAIL")
        print("The group-by results do not match.")


    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_GROUP_BY_AGGREGATION,
        elapsed_time=csv_elapsed_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_GROUP_BY_AGGREGATION,
        elapsed_time=parquet_elapsed_time,
    )


def run_pyspark_column_projection(
        runner: PySparkRunner,
        benchmark: Benchmark,
        csv_dataframe: DataFrame,
        parquet_dataframe: DataFrame,
) -> None:
    """
    Run the PySpark column projection workload.

    Selects only the type, amount and isFraud columns from
    the complete DataFrame.
    """

    print_section("PYSPARK COLUMN PROJECTION")

    print("\nTesting CSV...")

    (
        csv_results,
        csv_elapsed_time,
    ) = runner.column_projection(csv_dataframe)

    print(f"CSV time: {csv_elapsed_time:.3f} seconds")
    print("CSV columns:", csv_results.columns)
    print(f"CSV rows:   {csv_results.count():,}")

    print("\nTesting Parquet...")

    (
        parquet_results,
        parquet_elapsed_time,
    ) = runner.column_projection(parquet_dataframe)

    print(f"Parquet time: {parquet_elapsed_time:.3f} seconds")
    print("Parquet columns:", parquet_results.columns)
    print(f"Parquet rows: {parquet_results.count():,}")

    print_section("PYSPARK COLUMN PROJECTION RESULTS")

    print(f"CSV:     {csv_elapsed_time:.3f} seconds")
    print(f"Parquet: {parquet_elapsed_time:.3f} seconds")

    if (
        csv_results.columns == parquet_results.columns
        and csv_results.count() == parquet_results.count()
    ):
        print("\nStatus: PASS")
        print("Both formats produced the same projection.")
    else:
        print("\nStatus: FAIL")
        print("The projection results do not match.")


    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_COLUMN_PROJECTION,
        elapsed_time=csv_elapsed_time,
    )
    
    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_COLUMN_PROJECTION,
        elapsed_time=parquet_elapsed_time,
    )


def run_pyspark_column_projection_load(
        runner: PySparkRunner,
        benchmark: Benchmark,
) -> None:
    """
    Run the PqSpark column projection load workload.

    Loads only the type, amount and isFraud columns directly
    from the CSV and Parquet files.
    """

    print_section("PYSPARK COLUMN PROJECTION LOAD")

    print("\nLoading selected CSV columns...")

    (
        csv_results,
        parquet_results,
        csv_elapsed_time,
        parquet_elapsed_time,
    ) = runner.load_selected_columns()

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_COLUMN_PROJECT_LOAD,
        elapsed_time=csv_elapsed_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_COLUMN_PROJECT_LOAD,
        elapsed_time=parquet_elapsed_time,
    )


    print(f"CSV time:     {csv_elapsed_time:.3f} seconds")
    print(f"Parquet time: {parquet_elapsed_time:.3f} seconds")

    print("CSV columns:", csv_results.columns)
    print("Parquet columns:", parquet_results.columns)

    print(f"CSV rows:     {csv_results.count():,}")
    print(f"Parquet rows: {parquet_results.count():,}")

    print_section("PYSPARK COLUMN PROJECTION LOAD RESULTS")

    print(f"CSV:     {csv_elapsed_time:.3f} seconds")
    print(f"Parquet: {parquet_elapsed_time:.3f} seconds")

    if (
        csv_results.columns == parquet_results.columns
        and csv_results.count() == parquet_results.count()
    ):
        print("\nStatus: PASS")
        print("Both formats produced the same projection.")
    else:
        print("\nStatus: FAIL")
        print("The projection results do not match.")


def run_pyspark_filter_and_projection(
        runner: PySparkRunner,
        benchmark: Benchmark,
        csv_dataframe: DataFrame,
        parquet_dataframe: DataFrame,
) -> None:
    """
    Run the PySpark filter-and-projection workload.

    Filters fraudulent transactions and selects only the type
    and amount columns.
    """

    print_section("PYSPARK FILTER AND PROJECT")

    print("\nFiltering and projecting CSV DataFrame...")

    (
        csv_results,
        csv_elapsed_time
    ) = runner.filter_and_projection(csv_dataframe)

    print(f"CSV time: {csv_elapsed_time:.3f} seconds")
    print(f"CSV rows returned: {csv_results.count():,}")
    print(f"CSV columns: {csv_results.columns}")

    print("\nFiltering and projecting Parquet DataFrame...")

    (
        parquet_results,
        parquet_elapsed_time,
    ) = runner.filter_and_projection(parquet_dataframe)


    print(f"Parquet time: {parquet_elapsed_time:.3f} seconds")
    print(f"Parquet rows returned: {parquet_results.count():,}")
    print(f"Parquet columns: {parquet_results.columns}")

    print_section("PYSPARK FILTER AND PROJECT RESULTS")

    print(f"CSV:     {csv_elapsed_time:.3f} seconds")
    print(f"Parquet: {parquet_elapsed_time:.3f} seconds")

    if (
        csv_results.columns == parquet_results.columns
        and csv_results.count() == parquet_results.count()
    ):
        print("\nStatus: PASS")
        print("Both formats produced the same filter-and-project result.")
    else:
        print("\nStatus: FAIL")
        print("The filter-and-project results do not match.")


    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FILTER_AND_PROJECTION,
        elapsed_time=csv_elapsed_time,
    )

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FILTER_AND_PROJECTION,
        elapsed_time=parquet_elapsed_time,
    )