"""
pyspark_runnung.py

Provides the PySparkRunner class used to benchmark the transaction
dataset using PySpark with CSV and Parquet files.
"""

from pathlib import Path
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, sum
from pyspark.sql.types import(
    DoubleType,
    # IntegerType,
    LongType,
    StringType,
    StructField,
    StructType
)



class PySparkRunner:
    """
    Runs the benchmark workloads using PySpark.
    
    CSV and Parquet are both tested using the same Spark DataFrame
    operations so that the file format is the main difference between
    the two measurements.
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
        Initialise the PySpark runner.

        Parameters
        ----------
        spark: SparkSession
            Active Spark session used to process the dataset.

        csv_path: Path
            Path to the CSV dataset.

        parquet_path: Path
            Path to the Parquet dataset.
        """

        self.spark = spark
        self.csv_path = csv_path
        self.parquet_path = parquet_path


    def load_csv(self) -> tuple[DataFrame, float]:
        """
        Load the complete CSV dataset and measure the execution time.

        Returns
        -------
        tuple[DataFrame, float]
            The loaded DataFrame and elapsed time in seconds.
        """

        start_time = perf_counter()

        dataframe = (
            self.spark.read
            .option("header", True)
            .schema(self.TRANSACTION_SCHEMA) # .option("inferSchema", True)
            .csv(str(self.csv_path))
        )

        # Spark uses lazy evaluation, so count() forces the read
        # to actually execute before we stop the timer.
        dataframe.count()

        elapsed_time = perf_counter() - start_time

        return dataframe, elapsed_time


    def load_parquet(self) -> tuple[DataFrame, float]:
        """
        Load the complete Parquet dataset and measure the execution time.

        Returns
        -------
        tuple[DataFrame, float]
            The loaded DataFrame and elapsed time in seconds.
        """

        start_time = perf_counter()

        dataframe = self.spark.read.parquet(
            str(self.parquet_path)
        )

        # Force Spark to execute the read.
        dataframe.count()

        elapsed_time = perf_counter() - start_time

        return dataframe, elapsed_time


    def filter_and_aggregation(
            self,
            dataframe: DataFrame,
    ) -> tuple[int, float, float]:
        """
        Filter fraudulent transactions and calculate their count
        and total amount.
        
        Parameters
        ----------
        dataframe: DataFrame
            Transaction DataFrame to analyse.

        Returns
        -------
        tuple[int, float, float]
            Fraud transaction count, total fraud amount, and
            elapsed time in seconds.
        """

        start_time = perf_counter()

        results = (
            dataframe
            .filter(col("isFraud") == 1)
            .agg(
                count("*").alias("fraud_count"),
                sum("amount").alias("total_fraud_amount"),
            )
            .collect()[0]
        )

        fraud_count = results["fraud_count"]
        total_fraud_amount = results["total_fraud_amount"]

        elapsed_time = perf_counter() - start_time

        return (
            fraud_count,
            total_fraud_amount,
            elapsed_time,
        )


    def group_by_aggregation(
        self,
        dataframe: DataFrame,
    ) -> tuple[DataFrame, float]:
        """
        Group transaction by type and calculate aggregate values.

        Parameters
        ----------
        dataframe : DataFrame
            Transaction DataFrame to analyse.

        Returns
        -------
        tuple[DataFrame, float]
            Aggregated DataFrame and elapsed time in seconds.
        """

        start_time = perf_counter()

        results = (
            dataframe
            .groupby("type")
            .agg(
                count("amount").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("average_amount"),
                sum("isFraud").alias("fraud_count"),
            )
        )

        # Force the aggregation to execute.
        results.collect()

        elapsed_time = perf_counter() - start_time

        return results, elapsed_time


    def column_projection(
        self,
        dataframe: DataFrame,
    ) -> tuple[DataFrame, float]:
        """
        Select the columns used by the column projection workload.

        Parameters
        ----------
        dataframe : DataFrame
            Transaction DataFrame to analyse.

        Returns
        -------
        tuple[DataFrame, float]
            Projected DataFrame and elapsed time in seconds.
        """

        start_time = perf_counter()

        results = dataframe.select(
            "type",
            "amount",
            "isFraud",
        )

        # Force the projection to execute.
        results.count()

        elapsed_time = perf_counter() - start_time

        return results, elapsed_time


    # Direct disk access test
    def load_selected_columns(
            self,
    ) -> tuple[DataFrame, DataFrame, float, float]:
        """
        Load only the selected columns directly from CSV and Parquet.
        """

        selected_columns = [
            "type",
            "amount",
            "isFraud",
        ]

        # CSV

        start_time = perf_counter()

        csv_dataframe = (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(self.csv_path))
            .select(*selected_columns)
        )

        csv_dataframe.count()

        csv_elapsed_time = perf_counter() - start_time

        # PARQUET

        start_time = perf_counter()

        parquet_dataframe = (
            self.spark.read
            .parquet(str(self.parquet_path))
            .select(*selected_columns)
        )

        parquet_dataframe.count()

        parquet_elapsed_time = perf_counter() - start_time

        return (
            csv_dataframe,
            parquet_dataframe,
            csv_elapsed_time,
            parquet_elapsed_time,
        )


    def filter_and_projection(
        self,
        dataframe: DataFrame,
    ) -> tuple[DataFrame, float]:
        """
        Filter fraudulent transactions and select the required columns.

        Parameters
        ----------
        dataframe : DataFrame
            Transaction DataFrame to analyse.

        Returns
        -------
        tuple[DataFrame, float]
            Filtered and projected DataFrame and elapsed time
            in seconds.
        """

        start_time = perf_counter()

        results = (
            dataframe
            .filter(col("isFraud") == 1)
            .select(
                "type",
                "amount",
            )
        )

        # Force the filter and projection to execute.
        results.count()

        elapsed_time = perf_counter() - start_time

        return results, elapsed_time