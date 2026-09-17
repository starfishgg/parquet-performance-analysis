# Parquet Performance Analysis

A data engineering project investigating the performance differences between **CSV and Parquet** when processing a large transaction dataset with **Pandas, PySpark and DuckDB**.

The project uses the same underlying dataset in both CSV and Parquet formats so that the performance comparisons can be made under consistent conditions.

AIML dataset:
===========================
step            LONG
type	        VARCHAR
amount	        DOUBLE
nameOrig    	VARCHAR
oldbalanceOrg	DOUBLE
newbalanceOrig	DOUBLE
nameDest	    VARCHAR
oldbalanceDest	DOUBLE
newbalanceDest	DOUBLE
isFlaggedFraud	LONG
isFraud	        LONG
===========================

The AIML Fraud dataset can be downloaded in CSV format from:
https://www.kaggle.com/datasets/amanalisiddiqui/fraud-detection-dataset
You will need to convert it to parquet format yourself.

## Project Goals

The project will investigate how storage format and processing engine affect:

* File size
* Data loading time
* Column projection
* Filtering
* Aggregation
* Analytical query performance

The eventual benchmark will compare six combinations:

| Engine  | CSV | Parquet |
| ------- | :-: | :-----: |
| Pandas  |  ✓  |    ✓    |
| PySpark |  ✓  |    ✓    |
| DuckDB  |  ✓  |    ✓    |

The same analytical workloads will be applied to each combination.

## Dataset

The project uses a large financial transaction dataset containing approximately **6.36 million transactions**.

The dataset contains 11 columns including:

* Transaction type
* Transaction amount
* Origin and destination accounts
* Account balances
* Fraud indicators

The CSV and Parquet files contain the same underlying data.

The dataset files are **not included in this repository** because of their size.

Expected files:

```text
data/
├── AIML Dataset.csv
└── AIML Dataset.parquet
```

## Current Progress

### Milestone 1 — PySpark data validation

Completed.

PySpark successfully loads both the CSV and Parquet datasets using an explicit schema for the CSV file.

Current validation confirms:

* CSV rows: **6,362,620**
* Parquet rows: **6,362,620**
* CSV and Parquet schemas match
* Dataset validation: **PASS**

### Milestone 2 — Pandas analysis

Completed initial Pandas workloads.

Initial results on the local development machine:

| Workload                     |     CSV |     Parquet |
| ---------------------------- | ------: | ----------: |
| Full dataset load            | 7.678 s | **0.365 s** |
| Fraud analysis               | 0.013 s |     0.083 s |
| Transaction type aggregation | 0.249 s |     0.365 s |
| Three-column load            | 2.969 s | **0.112 s** |

The full dataset is approximately:

| Format  |      Size |
| ------- | --------: |
| CSV     | 470.67 MB |
| Parquet | 252.90 MB |

These timings are preliminary single-run measurements and are **not intended to represent final benchmark results**. Later stages will introduce repeated measurements and more controlled benchmarking.

## Planned Work

The project will progressively add:

1. Pandas CSV vs Parquet analysis
2. DuckDB CSV vs Parquet analysis
3. PySpark CSV vs Parquet analysis
4. A common benchmark structure
5. Repeated measurements and result validation
6. Benchmark result storage and analysis
7. Parquet compression comparisons
8. Partitioned Parquet experiments
9. Spark execution-plan analysis

## Project Structure

```text
parquet-performance-analysis/
│
├── data/
│   ├── AIML Dataset.csv
│   └── AIML Dataset.parquet
│
├── src/
│   ├── data_loader.py
│   └── pandas_runner.py
│
├── results/
│
├── main.py
├── requirements.txt
├── README.md
└── .gitignore
```

The dataset files and generated benchmark results are excluded from version control.

## Technologies

* Python
* Pandas
* DuckDB
* PySpark

## Purpose

This project is intended as a practical exploration of data-engineering concepts including columnar storage, analytical processing, file formats, query performance and distributed data processing.

