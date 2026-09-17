"""
pyspark_tests.py

Provides functions for running and diplaying the PySpark
performance tests.
"""

from src.benchmark import Benchmark
from src.pyspark_runner import PySparkRunner
from src.utils import print_section




def run_pyspark_full_read_benchmark(
    runner: PySparkRunner,
    benchmark: Benchmark,
):
    """
    Run the initial PqSpark full-read benchmark.

    This currently measures only the complete CSV and Parquet reads.
    """

    print_section("PYSPARK FULL READ TEST")

    print("\nLoading CSV with PySpark...")

    csv_dataframe, csv_load_time = runner.load_csv()

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_CSV,
        workload=Benchmark.WORKLOAD_FULL_READ,
        elapsed_time=csv_load_time,
    )

    print(f"CSV load time: {csv_load_time:.3f} seconds")
    print(f"CSV rows:      {csv_dataframe.count():,}")

    print("\nLoading Parquet with PySpark...")

    parquet_dataframe, parquet_load_time = runner.load_parquet()

    benchmark.create_result(
        engine=Benchmark.ENGINE_PYSPARK,
        file_format=Benchmark.FORMAT_PARQUET,
        workload=Benchmark.WORKLOAD_FULL_READ,
        elapsed_time=parquet_load_time,
    )

    print(f"Parquet load time: {parquet_load_time:.3f} seconds")
    print(f"Parquet rows:      {parquet_dataframe.count():,}")


    
