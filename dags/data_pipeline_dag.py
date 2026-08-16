"""
data_pipeline_dag.py
--------------------
Apache Airflow DAG for automating a data pipeline workflow.

Pipeline stages:
    extract >> validate >> process >> report >> notify

DAG ID   : data_pipeline_dag
Schedule : @daily
Catchup  : False
"""

import os
import logging
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Project root resolution
# ---------------------------------------------------------------------------
# The DAG file lives at  <PROJECT_ROOT>/dags/data_pipeline_dag.py
# So PROJECT_ROOT is one level above the directory that contains this file.
# This approach is cross-platform and works both on Windows and WSL.
_DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(_DAG_DIR)

# Convenience path helpers
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

INPUT_CSV = os.path.join(DATA_DIR, "input.csv")
EXTRACTED_CSV = os.path.join(OUTPUT_DIR, "extracted_data.csv")
PROCESSED_CSV = os.path.join(OUTPUT_DIR, "processed_data.csv")
VALIDATION_REPORT = os.path.join(OUTPUT_DIR, "validation_report.txt")
SUMMARY_REPORT = os.path.join(OUTPUT_DIR, "summary_report.txt")

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "mlao_student",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ===========================================================================
# TASK 1 — DATA EXTRACTION
# ===========================================================================

def extract() -> None:
    """
    Read data/input.csv, validate its existence, log record count,
    and save a copy to output/extracted_data.csv.

    Raises
    ------
    FileNotFoundError
        If data/input.csv does not exist.
    """
    log.info("=== TASK: extract ===")
    log.info("Looking for input file at: %s", INPUT_CSV)

    # Fail clearly if the input file is missing
    if not os.path.isfile(INPUT_CSV):
        raise FileNotFoundError(
            f"Input file not found: {INPUT_CSV}. "
            "Please place 'input.csv' inside the 'data/' directory before running the DAG."
        )

    # Load the CSV
    df = pd.read_csv(INPUT_CSV)
    num_records = len(df)
    num_columns = len(df.columns)

    log.info("Extraction successful.")
    log.info("  Records extracted : %d", num_records)
    log.info("  Columns found     : %d  -> %s", num_columns, list(df.columns))

    # Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Persist extracted data
    df.to_csv(EXTRACTED_CSV, index=False)
    log.info("Extracted data saved to: %s", EXTRACTED_CSV)


# ===========================================================================
# TASK 2 — DATA VALIDATION
# ===========================================================================

def validate() -> None:
    """
    Read the extracted dataset and perform a series of quality checks:
      - Record count
      - Column count
      - Missing values per column
      - Duplicate rows
      - Invalid values (e.g. negative performance scores)

    Results are logged and written to output/validation_report.txt.

    Raises
    ------
    ValueError
        If the dataset is critically invalid (zero records or zero columns).
    """
    log.info("=== TASK: validate ===")
    log.info("Reading extracted data from: %s", EXTRACTED_CSV)

    df = pd.read_csv(EXTRACTED_CSV)

    total_records = len(df)
    total_columns = len(df.columns)
    missing_per_column = df.isnull().sum()
    total_missing = int(missing_per_column.sum())
    duplicate_count = int(df.duplicated().sum())

    # ---- Detect obviously invalid values ----
    invalid_rows = {}
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    for col in numeric_cols:
        # Flag any negative values in columns expected to be non-negative
        if col.lower() in {"salary", "age", "performance_score", "score"}:
            neg_mask = df[col] < 0
            if neg_mask.any():
                invalid_rows[col] = int(neg_mask.sum())

    # ---- Determine validation status ----
    is_critical = total_records == 0 or total_columns == 0
    has_warnings = total_missing > 0 or duplicate_count > 0 or bool(invalid_rows)
    validation_status = (
        "CRITICAL FAILURE" if is_critical else ("WARNINGS" if has_warnings else "PASSED")
    )

    # ---- Log summary ----
    log.info("Validation Results:")
    log.info("  Total records   : %d", total_records)
    log.info("  Total columns   : %d", total_columns)
    log.info("  Missing values  : %d", total_missing)
    log.info("  Duplicate rows  : %d", duplicate_count)
    log.info("  Invalid values  : %s", invalid_rows if invalid_rows else "None detected")
    log.info("  Status          : %s", validation_status)

    if missing_per_column.any():
        log.info("  Missing breakdown:")
        for col, cnt in missing_per_column[missing_per_column > 0].items():
            log.info("    %-25s : %d missing", col, cnt)

    # ---- Write validation report ----
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(VALIDATION_REPORT, "w", encoding="utf-8") as f:
        f.write("=" * 60 + "\n")
        f.write("DATA VALIDATION REPORT\n")
        f.write(f"Generated at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Total Records   : {total_records}\n")
        f.write(f"Total Columns   : {total_columns}\n")
        f.write(f"Missing Values  : {total_missing}\n")
        f.write(f"Duplicate Rows  : {duplicate_count}\n\n")
        f.write("Missing Values per Column:\n")
        for col, cnt in missing_per_column.items():
            f.write(f"  {col:<25} : {cnt}\n")
        f.write("\nInvalid Values Detected:\n")
        if invalid_rows:
            for col, cnt in invalid_rows.items():
                f.write(f"  {col:<25} : {cnt} row(s) with negative values\n")
        else:
            f.write("  None detected\n")
        f.write(f"\nValidation Status : {validation_status}\n")

    log.info("Validation report written to: %s", VALIDATION_REPORT)

    # Fail the task if the data is critically invalid
    if is_critical:
        raise ValueError(
            f"Dataset is critically invalid: {total_records} records, "
            f"{total_columns} columns. Pipeline cannot continue."
        )


# ===========================================================================
# TASK 3 — DATA PROCESSING
# ===========================================================================

def process() -> None:
    """
    Clean and transform the extracted dataset:
      1. Remove exact duplicate rows.
      2. Drop rows where all values are NaN.
      3. Fill missing numeric values with column median.
      4. Fill missing categorical values with 'Unknown'.
      5. Remove rows with invalid (negative) values in non-negative columns.
      6. Reset the index and save to output/processed_data.csv.

    Logs record counts before and after processing.
    """
    log.info("=== TASK: process ===")
    log.info("Reading extracted data from: %s", EXTRACTED_CSV)

    df = pd.read_csv(EXTRACTED_CSV)
    records_before = len(df)
    log.info("Records before processing : %d", records_before)

    # Step 1: Remove exact duplicate rows
    before_dedup = len(df)
    df = df.drop_duplicates()
    removed_duplicates = before_dedup - len(df)
    if removed_duplicates:
        log.info("  Removed %d duplicate row(s).", removed_duplicates)

    # Step 2: Drop rows where ALL values are NaN (completely empty rows)
    df = df.dropna(how="all")

    # Step 3: Fill missing numeric columns with median
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    for col in numeric_cols:
        if df[col].isnull().any():
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            log.info("  Filled missing '%s' with median value: %.2f", col, median_val)

    # Step 4: Fill missing categorical/object columns with 'Unknown'
    object_cols = df.select_dtypes(include="object").columns.tolist()
    for col in object_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna("Unknown")
            log.info("  Filled missing '%s' with 'Unknown'.", col)

    # Step 5: Remove rows with negative values in columns that must be positive
    positive_cols = [
        c for c in numeric_cols
        if c.lower() in {"salary", "age", "performance_score", "score"}
    ]
    invalid_mask = pd.Series([False] * len(df), index=df.index)
    for col in positive_cols:
        invalid_mask |= df[col] < 0
    removed_invalid = int(invalid_mask.sum())
    if removed_invalid:
        df = df[~invalid_mask]
        log.info("  Removed %d row(s) with invalid (negative) values.", removed_invalid)

    # Step 6: Reset index for a clean sequential index
    df = df.reset_index(drop=True)

    records_after = len(df)
    log.info("Records after processing  : %d", records_after)
    log.info("Processing status         : COMPLETED")

    # Save processed data
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(PROCESSED_CSV, index=False)
    log.info("Processed data saved to: %s", PROCESSED_CSV)


# ===========================================================================
# TASK 4 — REPORT GENERATION
# ===========================================================================

def report() -> None:
    """
    Read the processed dataset and the extracted dataset, then generate a
    human-readable summary report at output/summary_report.txt.

    Required report items:
      1. Number of records processed
      2. Missing values identified
      3. Processing completion status

    Additional details are also included for completeness.
    """
    log.info("=== TASK: report ===")

    # Load processed data for final statistics
    df_processed = pd.read_csv(PROCESSED_CSV)

    # Load extracted data to compare before/after
    df_extracted = pd.read_csv(EXTRACTED_CSV)
    records_extracted = len(df_extracted)
    records_processed = len(df_processed)
    records_removed = records_extracted - records_processed

    # Recompute missing values from the raw extracted data
    total_missing_raw = int(df_extracted.isnull().sum().sum())
    duplicate_count = int(df_extracted.duplicated().sum())
    num_columns = len(df_processed.columns)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Determine validation status label
    validation_status = (
        "WARNINGS" if (total_missing_raw > 0 or duplicate_count > 0) else "PASSED"
    )

    report_lines = [
        "=" * 60,
        "PIPELINE SUMMARY REPORT",
        f"Generated at : {timestamp}",
        "=" * 60,
        "",
        "--- REQUIRED METRICS ---",
        f"1. Records Processed          : {records_processed}",
        f"2. Missing Values Identified  : {total_missing_raw}",
        f"3. Processing Status          : COMPLETED SUCCESSFULLY",
        "",
        "--- ADDITIONAL DETAILS ---",
        f"Records Extracted             : {records_extracted}",
        f"Records Removed (cleaning)    : {records_removed}",
        f"Columns in Final Dataset      : {num_columns}",
        f"Duplicate Rows Found          : {duplicate_count}",
        f"Validation Status             : {validation_status}",
        "",
        "--- COLUMN SUMMARY (Processed Data) ---",
    ]

    for col in df_processed.columns:
        dtype = str(df_processed[col].dtype)
        report_lines.append(f"  {col:<25} dtype={dtype}")

    report_lines += [
        "",
        "--- PIPELINE TASK CHAIN ---",
        "  extract >> validate >> process >> report >> notify",
        "",
        "=" * 60,
        "End of Report",
        "=" * 60,
    ]

    report_text = "\n".join(report_lines)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(SUMMARY_REPORT, "w", encoding="utf-8") as f:
        f.write(report_text)

    log.info("Summary report written to: %s", SUMMARY_REPORT)
    log.info("Report preview:\n%s", report_text)


# ===========================================================================
# TASK 5 — NOTIFICATION
# ===========================================================================

def notify() -> None:
    """
    Log a completion notification after all pipeline stages have succeeded.
    No external services are used; console/log output is sufficient for
    this assignment.
    """
    log.info("=== TASK: notify ===")
    log.info("Workflow completed successfully!")
    log.info("Pipeline outputs are available in: %s", OUTPUT_DIR)
    log.info("  - extracted_data.csv")
    log.info("  - validation_report.txt")
    log.info("  - processed_data.csv")
    log.info("  - summary_report.txt")


# ===========================================================================
# DAG DEFINITION
# ===========================================================================

with DAG(
    dag_id="data_pipeline_dag",
    description="Automated data pipeline: extract -> validate -> process -> report -> notify",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlao", "assignment4", "data_pipeline"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    t_process = PythonOperator(
        task_id="process",
        python_callable=process,
    )

    t_report = PythonOperator(
        task_id="report",
        python_callable=report,
    )

    t_notify = PythonOperator(
        task_id="notify",
        python_callable=notify,
    )

    # -----------------------------------------------------------------------
    # Task dependency chain
    #   extract >> validate >> process >> report >> notify
    # -----------------------------------------------------------------------
    t_extract >> t_validate >> t_process >> t_report >> t_notify
