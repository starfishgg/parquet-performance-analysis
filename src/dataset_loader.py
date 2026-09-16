"""
dataset_loader.py

Provides the DatasetLoader class used to load and validate the
transaction dataset for the performance analysis project.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import(
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)


class DatasetLoader:
    """
    Loads the transaction dataset from CSV and Parquet.

    The CSV and Parquet files contain the same underlying data.

    CSV requires an explicit schema because CSV files do not store
    column data types. Parquet stores its schema with the data.
    """

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

    