"""
dataset_loader.py

Provides the DatasetLoader class used to load and validate the
transaction dataset for the performance analysis project.
"""

from pathlib import Path
import pandas as pd

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import(
    DoubleType,
    # IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)

from src.utils import benchmark_function




class DatasetLoader:
    """
    Loads and prepares the transaction dataset used by the
    performance analysis project.

    The class also provides utilities for creating smaller
    datasets for performance experiments.
    """

    # Schema for the Dataset, can be used by the CSV import code
    # to improve the speed at which the dataset is loaded as
    # the schema does not have to be inferred.
    TRANSACTION_SCHEMA = StructType([
        StructField("step", LongType(), True),
        StructField("type", StringType(), True),
        StructField("amount", DoubleType(), True),

        StructField("nameOrig", StringType(), True),
        StructField("oldbalanceOrg", DoubleType(), True),
        StructField("newbalanceOrig", DoubleType(), True),

        StructField("nameDest", StringType(), True),
        StructField("oldbalanceDest", DoubleType(), True),
        StructField("newbalanceDest", DoubleType(), True),

        StructField("isFraud", LongType(), True),
        StructField("isFlaggedFraud", LongType(), True),
    ])


    def __init__(
            self,
            spark: SparkSession,
            csv_path: Path,
            parquet_path: Path,
    ) -> None:
        """
        Initialise the dataset loader.

        Parameters
        ==========
        spark:
            Active Spark session.

        csv_path:
            Path to the CSV dataset.

        parquet_path:
            Path to the Parquet dataset.
        """

        self.spark = spark
        self.csv_path = csv_path
        self.parquet_path = parquet_path


    def load_csv(self) -> DataFrame:
        """
        Load the CSV dataset into a Spark DataFrame
        """

        return (
            self.spark.read
            .option("header", True)
            .schema(self.TRANSACTION_SCHEMA)
            .csv(str(self.csv_path))
        )


    def load_csv_with_inference(
        self,
        csv_path: Path,
        repetitions: int = 3,
    ) -> tuple[DataFrame, float]:
        """
        Load a CSV dataset using Spark schema inference.

        The dataset is loaded once as a warm-up, followed by the
        requested number of measured runs. The returned time is
        the median of the measured runs.
        """

        def load_dataset() -> DataFrame:
            dataframe = (
                self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(str(csv_path))
            )

            # Spark is lazy, so force the CSV read to execute.
            dataframe.count()

            return dataframe

        dataframe, elapsed_time = benchmark_function(
            load_dataset,
            repetitions,
        )

        return dataframe, elapsed_time


    def load_csv_with_schema(
        self,
        csv_path: Path,
        repetitions: int = 3,
    ) -> tuple[DataFrame, float]:
        """
        Load a CSV dataset using the explicitly declared schema.

        The dataset is loaded once as a warm-up, followed by the
        requested number of measured runs. The returned time is
        the median of the measured runs.
        """

        def load_dataset() -> DataFrame:
            dataframe = (
                self.spark.read
                .option("header", True)
                .schema(self.TRANSACTION_SCHEMA)
                .csv(str(csv_path))
            )

            # Spark is lazy so force the CSV read to execute.
            dataframe.count()

            return dataframe

        dataframe, elapsed_time = benchmark_function(
            load_dataset,
            repetitions,
        )

        return dataframe, elapsed_time


    def load_parquet(self) -> DataFrame:
        """
        Load the Parquet dataset into a Spark DataFrame.
        
        The schema is read directly fromt he Parquet metadata.
        """

        return self.spark.read.parquet(
            str(self.parquet_path)
        )


    def get_csv_size_mb(self) -> float:
        """
        Return the CSv file size in megabytes
        """

        return self._get_file_size_mb(self.csv_path)


    def get_parquet_size_mb(self) -> float:
        """
        Return the Parquet file size in megabytes
        """

        return self._get_file_size_mb(self.parquet_path)


    @staticmethod
    def _get_file_size_mb(file_path: Path) -> float:
        """
        Convert a file's size from bytes to megabytes.
        """

        size_bytes = file_path.stat().st_size

        return size_bytes / (1024 * 1024)

    
    def partition_dataset_file(self, file_path: Path) -> None:
        """
        Create smaller versions of the CSV dataset for testing
        how CSV loading performance changes with dataset size.

        The smaller files contain the first 10%, 25%, 50%, and 75%
        of the original dataset. Using the same starting rows for
        each file makes the datasets progressively larger versions
        of the same data.
        """

        total_rows = sum(
            1
            for _ in open(file_path, "r", encoding="utf-8")
        ) - 1

        percentages = [10, 25, 50, 75]

        output_directory = Path("data/resized")
        output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        for percentage in percentages:
            row_count = int(total_rows * percentage / 100)

            output_file = (
                output_directory
                / f"AIML Dataset {percentage}%.csv"
            )

            with (
                open(file_path, "r", encoding="utf-8") as input_file,
                open(output_file, "w", encoding="utf-8", newline="") as output_file_handle,
            ):
                header = input_file.readline()
                output_file_handle.write(header)

                for _ in range(row_count):
                    line = input_file.readline()

                    if not line:
                        break

                    output_file_handle.write(line)

            print(
                f"Created {output_file}: "
                f"{row_count:,} rows"
            )