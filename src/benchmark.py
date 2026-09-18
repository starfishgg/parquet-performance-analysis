"""
benchmark.py

Defines the analytical workloads used by the performance benchmark.
"""


from dataclasses import dataclass

from src.utils import print_section





@dataclass
class BenchmarkResult:
    """
    Stores the result of one benchmark measurement.
    """
    engine: str
    file_format: str
    workload: str
    elapsed_time: float


class Benchmark:
    """
    Defines the common analytical workloads used to compare
    Pandas, PySpark and DuckDB.
    """

    ENGINE_PANDAS = "Pandas"
    ENGINE_PYSPARK = "PySpark"
    ENGINE_DUCKDB = "DuckDB"

    FORMAT_CSV = "CSV"
    FORMAT_PARQUET = "Parquet"

    # Read the complete dataset
    WORKLOAD_FULL_READ = "full_read"
    # Filter by fraud, calc. count/total
    WORKLOAD_FILTER_AND_AGGREGATION = "filter_and_aggregation"
    # Group by type, calc. summary stats
    WORKLOAD_GROUP_BY_AGGREGATION = "group_by_aggregation"
    # Read only type, amount, isFraud
    WORKLOAD_COLUMN_PROJECTION = "column_projection"
    # Raed only type, amount, isFraud, but direct from disk, not memory
    WORKLOAD_COLUMN_PROJECT_LOAD = "column_projection_load"
    # Filter by fraud, return selected cols
    WORKLOAD_FILTER_AND_PROJECTION = "filter_and_projection"

    WORKLOADS = [
        WORKLOAD_FULL_READ,
        WORKLOAD_FILTER_AND_AGGREGATION,
        WORKLOAD_GROUP_BY_AGGREGATION,
        WORKLOAD_COLUMN_PROJECTION,
        WORKLOAD_FILTER_AND_PROJECTION,
    ]


    def __init__(self) -> None:
        self.results: list[BenchmarkResult] = []



    def get_workloads(self) -> list[str]:
        """
        Return the workloads included in the benchmark.
        
        Returns
        -------
        list[str]
            Names of the benchmark workloads.
        """

        return self.WORKLOADS.copy()


    def create_result(
            self,
            engine: str,
            file_format: str,
            workload: str,
            elapsed_time: float
    ) -> BenchmarkResult:
        """
        Create a benchmark result.

        Parameters
        ----------
        engine: str
            Processing engine used for the measurement.

        file_format: str
            Dataset file format used for the measurement.
        
        workload: str
            Benchmark workload being measured.

        elapsed_time: float
            Execution time in seconds.

        Returns
        -------
        BenchmarkResult
            The recorded benchmark result.
        """

        result = BenchmarkResult(
            engine=engine,
            file_format=file_format,
            workload=workload,
            elapsed_time=elapsed_time,
        )

        self.results.append(result)

        return result


    def remove_results_for_engine(self, engine: str) -> None:
        """
        Remove all previously recorded results for one engine.
        
        This allows a new run of an engine's benchmarks to replace
        its previosu results rather than creating duplicates.
        """

        self.results = [
            result
            for result in self.results
            if result.engine != engine
        ]


    def get_results(self) -> list[BenchmarkResult]:
        """
        Return all recorded benchmark results.

        Returns
        -------
        list[BenchmarkResult]
            Recorded benchmark results
        """

        return self.results.copy()


    def print_results(self) -> None:
        """
        Print all recorded benchmark results grouped by workload.
        """

        print_section("RECORDED BENCHMARK RESULTS")

        #results = self.get_results()

        if not self.results:
            print("No benchmark results have been recorded.")
            return

        workload_order = {
            workload: position
            for position, workload in enumerate(
                self.WORKLOADS
            )
        }

        sorted_results = sorted(
            self.results,
            key=lambda result: (
                workload_order.get(
                    result.workload,
                    len(workload_order))
                ,
                result.engine,
                result.file_format,
            ),
        )

        current_workload = None

        for result in sorted_results:
            if result.workload != current_workload:
                if current_workload is not None:
                    print()

                print(f"{result.workload}")

                print(
                    f"{'Engine':<12}"
                    f"{'Format':<10}"
                    f"{'Time (s)':>12}"
                )

                print("-" * 36)

                current_workload = result.workload

            print(
                f"{result.engine:<12}"
                f"{result.file_format:<10}"
                f"{result.elapsed_time:>12.3f}"
            )