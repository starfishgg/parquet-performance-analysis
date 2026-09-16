"""
pandas_runner.py

Provides the PandasRunner class used to compare CSV and Parquet
loading performance with Pandas.
"""

from pathlib import Path
from time import perf_counter

import pandas as pd




class PandasRunner:
    """
    Loads the transaction dataset using Pandas and measures
    how long each file format takes to load.
    """

    def __init__(
            self,
            csv_path: Path,
            parquet_path: Path,
    ) -> None:
        """
        Initialise the Pandas runner.
        
        Parameters
        ==========
        csv_path:
            Path to the CSV dataset.

        parquet_path
            Path to the Parquet dataset
        """

        self.csv_path = csv_path
        self.parquet_path = parquet_path


    def load_csv(self) -> tuple[pd.DataFrame, float]:
        """
        Load the CSV dataset and measure the loading time.
        
        Returns
        -------
        tuple[pd.DataFrame, float]
            The loaded DataFrame and elapsed time in seconds.
        """

        start_time = perf_counter()

        dataframe = pd.read_csv(self.csv_path)

        elapsed_time = perf_counter() - start_time

        return dataframe, elapsed_time


    def load_parquet(self) -> tuple[pd.DataFrame, float]:
        """
        Load the Parquet dataset and measure the loading time.
        
        Returns
        -------
        tuple[pd.DataFrame, float]
            The loaded DataFrame and elapsed time in seconds.
        """

        start_time = perf_counter()

        dataframe = pd.read_parquet(self.parquet_path)

        elapsed_time = perf_counter() - start_time

        return dataframe, elapsed_time


    def analyse_fraud(
            self,
            dataframe: pd.DataFrame,
    ) -> tuple[int, float, float]:
        """
        Find fraudulent transactions and calculate their total value.
        
        The operation:
        
        1. Filters the DataFrame to fraudulent transactions.
        2. Counts the fraudulent transactions.
        3. Calculates the total transaction amount.
        4. Measures the time taken.
        
        Parameters
        ----------
        dataframe:
            DataFrame containing the transaction data.
            
        Returns
        -------
        tuple[int, float, float]
            Fraud transaction count, total fraud amount,
            and elapsed time in seconds.
        """

        start_time = perf_counter()

        fraud_transactions = dataframe[dataframe["isFraud"] == 1]

        fraud_count = len(fraud_transactions)
        total_fraud_amount = fraud_transactions["amount"].sum()

        elapsed_time = perf_counter() - start_time

        return fraud_count, total_fraud_amount, elapsed_time


    def analyse_by_transaction_type(
            self,
            dataframe: pd.DataFrame,
    ) -> tuple[pd.DataFrame, float]:
        """
        Aggregate transaction data by transaction type.
        
        For each transaction type, calculate:
            - Number of transactions.
            - Total transaction amount.
            - Average transaction amount.
            - Number of fraudulent transactions.

        Parameters
        ----------
        dataframe:
            DataFrame containing the transaction data.


        Returns
        -------
        tuple[pd.DataFrame, float]
            Aggregated results and elapsed time in seconds.
        """

        start_time = perf_counter()

        results = (
            dataframe
            .groupby("type")
            .agg(
                transaction_count=("amount", "count"),
                total_amount=("amount", "sum"),
                average_amount=("amount", "mean"),
                fraud_count=("isFraud", "sum"),   # 0 or 1 column
            )
            .reset_index()
        )

        elapsed_time = perf_counter() - start_time

        return results, elapsed_time


    def analyse_selected_columns(
            self,
            dataframe: pd.DataFrame,
    ) -> tuple[pd.DataFrame, float]:
        """
        Select a small subset of columns and measure the operation time.

        This represents an analystical workload where only the columns
        required for the analysis are used (which should generally favour parquet's column-based storage, although we are currently reading from memory, not disk).

        Parameters
        ----------
        dataframe:
            DataFrame containing the transaction data.

        Returns
        -------
        tuple[pd.DataFrame, float]
            DataFrame containing the selected columns and elapsed timel
        """

        start_time = perf_counter()

        selected_columns = dataframe[
            ["type", "amount", "isFraud"]
        ].copy()

        elapsed_time = perf_counter() - start_time

        return selected_columns, elapsed_time


    def load_selected_columns(
            self,
    ) -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
        """
        Load only the columns required for a small analytical workload.

        CSV and Parquet are both asked for the same three columns, direct from disk, not memory.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame, float, float]
            CSV DataFrame, Parquet DataFrame, CSV load time,
            and Parquet load time.
        """

        selected_columns = [
            "type",
            "amount",
            "isFraud",
        ]

        start_time = perf_counter()

        csv_dataframe = pd.read_csv(
            self.csv_path,
            usecols=selected_columns,
        )

        csv_elapsed_time = perf_counter() - start_time

        start_time = perf_counter()

        parquet_dataframe = pd.read_parquet(
            self.parquet_path,
            columns=selected_columns,
        )

        parquet_elapsed_time = perf_counter() - start_time

        return (
            csv_dataframe,
            parquet_dataframe,
            csv_elapsed_time,
            parquet_elapsed_time,
        )