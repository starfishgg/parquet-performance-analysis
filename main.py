"""
main.py

Entry point for the Parquet Performance Analysis project.

The program:

1. Locates the CSV and Parquet datasets.
2. Reports their file sizes.
3. Loads both firunnerles with PySpark.
4. Validates their schemas and row counts.
5. Runs the Pandas workloads.
6. Runs the PySpark workloads.
7. Displays the recorded benchmark results.
"""


from pathlib import Path
from pyspark.sql import SparkSession

from src.benchmark import Benchmark
from src.dataset_loader import DatasetLoader
from src.pandas_runner import PandasRunner
from src.pyspark_runner import PySparkRunner
from src.pandas_tests import run_pandas_benchmark
from src.pyspark_tests import run_pyspark_benchmark
from src.utils import print_section




def get_dataset_paths() -> tuple[Path, Path]:
    """
    Return the paths to the CSV and Parquet datasets.
    
    Returns
    -------
    tuple[Path, Path]
        CSV path and PArquet path.
    """
    project_directory = Path(__file__).resolve().parent

    csv_path =     (project_directory / "data" / "AIML Dataset.csv")
    parquet_path = (project_directory / "data" / "AIML Dataset.parquet")

    return csv_path, parquet_path


def create_spark_session() -> SparkSession:
    """
    Create and configure the Spark session.
    
    Returns
    -------
    SparkSession
        Configures Spark session.
    """
    spark = (
        SparkSession.builder
        .appName("ParquetPerformanceAnalysis")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def report_dataset_files(
        loader: DatasetLoader,
        csv_path: Path,
        parquet_path: Path,
) -> None:
    """
    Display dataset paths and file sizes.
    """

    print(f"CSV:")
    print(f"  Path: {csv_path}")
    print(f"  Size: {loader.get_csv_size_mb():,.2f} MB")

    print(f"Parquet:")
    print(f"  Path: {parquet_path}")
    print(f"  Size: {loader.get_parquet_size_mb():,.2f} MB")


def validate_datasets(loader: DatasetLoader) -> None:
    """
    Load both datasets with PySpark and validate their row counts.
    """

    print_section("LOADING CSV")
    csv_dataframe = loader.load_csv()
    csv_dataframe.printSchema()
    csv_row_count = csv_dataframe.count()
    print(f"CSV row count: {csv_row_count:,}")

    print_section("LOADING PARQUET")
    parquet_dataframe = loader.load_parquet()
    parquet_dataframe.printSchema()
    parquet_row_count = parquet_dataframe.count()
    print(f"Parquet row count: {parquet_row_count:,}")

    print_section("DATA VALIDATION")
    print(f"CSV rows:     {csv_row_count:,}")
    print(f"Parquet rows: {csv_row_count:,}")

    if csv_row_count == parquet_row_count:
        print("\nStatus: PASS")
        print("Both files contain the same number of rows.")
    else:
        print("\nStatus: FAIL")
        print("The CSV and PArquet row counts do not match.")


def print_benchmark_results(
    benchmark: Benchmark,
) -> None:
    """
    Display all recorded benchmark results.
    """
    print_section("RECORDED BENCHMARK RESULTS")

    for result in benchmark.get_results():
        print(result)


def run_schema_loading_test(loader: DatasetLoader) -> None:
    """
    Compare CSV loading performance using schema inference
    and an explicitly declared schema across different
    dataset sizes.
    """

    print_section("CSV SCHEMA LOADING TEST")

    path_string = "data/resized/AIML Dataset "

    dataset_sizes = [
        ( "10%", Path(path_string +  "10%.csv")),
        ( "25%", Path(path_string +  "25%.csv")),
        ( "50%", Path(path_string +  "50%.csv")),
        ( "75%", Path(path_string +  "75%.csv")),
        ("100%", loader.csv_path),
    ]

    for size_label, csv_path in dataset_sizes:
        print()
        print(f"Dataset size: {size_label}")

        _, inference_time = loader.load_csv_with_inference(csv_path)

        _, schema_time = loader.load_csv_with_schema(csv_path)

        print(f"Schema inference: {inference_time:.3f} seconds")

        print(f" Explicit schema: {schema_time:.3f} seconds")


    print_section("PARQUET PERFORMANCE ANALYSIS")

    report_dataset_files(
        loader=loader,
        csv_path=loader.csv_path,
        parquet_path=loader.parquet_path,
    )

    validate_datasets(loader)


def run_menu(
    loader: DatasetLoader,
    pandas_runner: PandasRunner,
    pyspark_runner: PySparkRunner,
    benchmark: Benchmark,
) -> None:
    """
    Display the benchmarh menu and run the selected test.
    """

    while True:
        print_section("DATA PROCESSING PERFORMANCE ANALYSIS")

        print("\t1. Run CSV Schema loading test (NOT BENCHMARKED)")
        print("\t2. Run Pandas benchmarks")
        print("\t3. Run PySpark benchmarks")
        print("\t4. Run DuckDB benchmarks (NOT YET IMPLEMENTED)")
        print("\t5. Run all available benchmarks")
        print("\t6. Print Benchmark Results")
        print("\t7. Exit (or 'q')")

        choice = input("\nSelect an options: ")

        if choice == "1":
            run_schema_loading_test(loader)
        elif choice == "2":
            run_pandas_benchmark(pandas_runner, benchmark)
        elif choice == "3":
            run_pyspark_benchmark(pyspark_runner, benchmark)
        elif choice == "4":
            pass
            #run_duckdb_benchmark()
        elif choice == "5":
            run_pandas_benchmark(pandas_runner, benchmark)
            run_pyspark_benchmark(pyspark_runner, benchmark)
        elif choice == "6":
            benchmark.print_results()
        elif choice == "7" or choice == "q":
            break
        else:
            print("Invalid selection.")
            




def main() -> None:
    """
    Run the Parquet Performance Analysis Project
    """

    benchmark = Benchmark()

    csv_path, parquet_path = get_dataset_paths()

    spark = create_spark_session()

    loader = DatasetLoader(
        spark=spark,
        csv_path=csv_path,
        parquet_path=parquet_path,
    )

    pandas_runner = PandasRunner(
        csv_path, 
        parquet_path,
    )

    pyspark_runner = PySparkRunner(
        spark=spark,
        csv_path=csv_path,
        parquet_path=parquet_path,
    )

    try:
        run_menu(
            loader = loader,
            pandas_runner = pandas_runner,
            pyspark_runner = pyspark_runner,
            benchmark = benchmark,
            )        

    finally:
        spark.stop()



    
if __name__ == "__main__":
    main()


