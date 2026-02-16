"""
Sales ETL Pipeline
==================
Main orchestration module for the sales data ETL process.

Pipeline Flow:
1. Check for failed previous runs
2. Extract files from S3
3. Validate file types and schemas
4. Enrich data with dimension tables
5. Create customer and sales data marts
6. Calculate business metrics
7. Move processed files and update tracking
"""

from datetime import datetime
import config
import os
from src.utils.logging_config import *
from src.utils.utility import *
from src.move.move import *
from src.extract.extract import *
from src.staging.staging import check_already_processed_files, insert_staging_records, update_staging_table
from src.transform.transform import (
    customer_monthly_sales_calculate, 
    dimesions_table_join, 
    sales_team_incentive_calculate, 
    validate_df, 
    validate_schema
)
from src.load.write import (
    df_write_to_local_parquet_partitioned, 
    df_write_to_s3_parquet_partitioned, 
    write_parquet_to_local, 
    write_parquet_to_local_partitioned, 
    write_parquet_to_s3, 
    write_parquet_to_s3_partitioned
)


# ============================================================================
# PIPELINE STEP 1: PRE-PROCESSING CHECKS
# ============================================================================

def check_previous_run_failure(connection):
    """
    Check for files from previous failed pipeline runs.
    
    Queries staging table to identify files that were marked as 'START'
    but never completed, indicating a pipeline failure.
    
    Args:
        connection: MySQL database connection
        
    Raises:
        Exception: If files are found with 'START' status (previous run failed)
    """
    logger.info(f"{'*' * 8} Step 1: Checking previous failed runs {'*' * 8}")

    # Get all CSV files currently in local directory
    local_csv_files = [
        file for file in os.listdir(config.local_directory) 
        if file.endswith(".csv")
    ]
    
    # Query to check if these files are stuck in 'START' status
    check_query_statement = (
        f"SELECT DISTINCT file_name FROM "
        f"{config.database_name}.{config.staging_table_name} "
        f"WHERE file_name IN ({str(local_csv_files)[1:-1]}) "
        f"AND status = 'START'"
    )
    
    if local_csv_files:
        logger.info(f"{'*' * 8} Connecting to database {'*' * 8}")
        cursor = connection.cursor()
        cursor.execute(check_query_statement)
        data = cursor.fetchall()

        if data:
            logger.error("Previous run failed - files stuck in START status")
            raise Exception("Previous pipeline run incomplete")
        else:
            logger.warning("Unknown files found in local directory\n")
    else:
        logger.info("Last run was successful - no leftover files\n")


def move_unknown_files():
    """
    Move any leftover files in local directory to unknown folder.
    
    This ensures the local directory is clean before processing new files.
    Any files present at pipeline start are considered 'unknown' and moved
    to a separate folder for investigation.
    """
    all_files = [file for file in os.listdir(config.local_directory)]
    
    if all_files:
        logger.info("Moving unknown files from local directory")
        source_dir = config.local_directory
        destination_dir = config.unknown_directory
        move_local_to_local_folder(source_dir, destination_dir)
    else:
        logger.info("No unknown files found - directory is clean")


# ============================================================================
# PIPELINE STEP 2: DATA EXTRACTION
# ============================================================================

def extract_files_from_s3(bucket_name, s3_client):
    """
    Extract files from S3 source folder to local directory.
    
    Downloads all files from the configured S3 'to_process' folder
    to the local working directory for processing.
    
    Args:
        bucket_name (str): S3 bucket name
        s3_client: Boto3 S3 client instance
        
    Returns:
        list: List of local file paths that were downloaded
        
    Raises:
        Exception: If no files are found in S3 source folder
    """
    logger.info(f"{'*' * 8} Step 2: Extracting files from S3 to local {'*' * 8}")

    # List all files in S3 source directory
    s3_files_with_prefix = list_files(
        bucket_name, 
        config.s3_to_process_directory, 
        s3_client
    )
    
    if s3_files_with_prefix:
        # Download files to local directory
        local_files = download_from_s3(
            bucket_name, 
            s3_files_with_prefix, 
            config.local_directory, 
            s3_client
        )
        logger.info(f"{'-' * 8} Downloaded {len(local_files)} files from S3 {'-' * 8}\n")
        return local_files
    else:
        logger.error(f"{'-' * 8} No files found in S3 source {'-' * 8}")
        raise Exception("No data available in S3 to process")


# ============================================================================
# PIPELINE STEP 3: FILE TYPE VALIDATION
# ============================================================================

def separate_csv_and_wrong_files(local_files):
    """
    Separate CSV files from non-CSV files.
    
    Only CSV files are valid for processing. Any other file types
    are considered errors and will be moved to error folder.
    
    Args:
        local_files (list): List of downloaded file paths
        
    Returns:
        tuple: (csv_files, wrong_files) - Two lists of file paths
    """
    csv_files = []
    wrong_files = []

    if local_files:
        for file in local_files:
            if file.endswith('.csv'):
                csv_files.append(file)
            else:
                wrong_files.append(file)
    
    logger.info(f"Found {len(csv_files)} CSV files and {len(wrong_files)} non-CSV files")
    return csv_files, wrong_files


def handle_wrong_file_types(wrong_files, bucket_name, s3_client):
    """
    Move non-CSV files to error folders (both local and S3).
    
    Files with incorrect extensions are moved to dedicated error folders
    for investigation and are excluded from processing.
    
    Args:
        wrong_files (list): List of non-CSV file paths
        bucket_name (str): S3 bucket name
        s3_client: Boto3 S3 client instance
    """
    if wrong_files:
        logger.info(f"Moving {len(wrong_files)} wrong file type(s) to error folder")
        
        # Move files locally
        move_files_locally(wrong_files, config.wrong_directory)
        
        # Move files in S3
        move_files_s3_to_s3(
            wrong_files, 
            bucket_name, 
            config.s3_to_process_directory, 
            config.s3_wrong_directory, 
            s3_client
        )
        logger.info("Wrong file types moved successfully")
    else:
        logger.info("No wrong file types found")

# ============================================================================
# PIPELINE STEP 3.1: IDEMPOTENCY CHECK
# ============================================================================

def filter_already_processed_files(csv_files,db_name,bucket_name,staging_table_name,s3_client):
    logger.info("=" * 60)
    logger.info("Step 4.5: Checking for already processed files (Idempotency)")
    logger.info("=" * 60)
        
    files_to_process, already_processed_files = check_already_processed_files(
        csv_files,
        db_name,
        staging_table_name
    )
        
    # Move already processed files to a separate folder
    if already_processed_files:
        logger.info(f"Moving {len(already_processed_files)} already processed files")
        move_files_locally(already_processed_files, config.already_processed_directory)
        move_files_s3_to_s3(
            already_processed_files,
            bucket_name,
            config.s3_to_process_directory,
            config.s3_already_processed_directory,
            s3_client
        )
        
    # Check if any files remain to process
    if not files_to_process:
        logger.warning("=" * 60)
        logger.warning("No new files to process - all files already completed")
        logger.warning("=" * 60)
        return  # Exit pipeline gracefully    
    return files_to_process


# ============================================================================
# PIPELINE STEP 4: SCHEMA VALIDATION
# ============================================================================

def validate_and_separate_files(spark, csv_files, bucket_name, s3_client):
    """
    Validate CSV file schemas and separate valid/invalid files.
    
    Checks each CSV file against mandatory column requirements.
    Files with missing columns are moved to error folder.
    Files with extra columns have them merged into 'additional_column'.
    Valid files are union'd into a single DataFrame.
    
    Args:
        spark: Spark session
        csv_files (list): List of CSV file paths to validate
        bucket_name (str): S3 bucket name
        s3_client: Boto3 S3 client instance
        
    Returns:
        tuple: (final_df, valid_files)
            - final_df: Union of all valid files as Spark DataFrame
            - valid_files: List of valid file paths
            
    Raises:
        Exception: If no valid files remain after validation
    """
    logger.info("Validating schema of CSV files against mandatory columns")
    
    # Validate schemas and merge valid files
    final_df_to_process, wrong_schema_csv_files, valid_schema_csv_files = validate_schema(
        spark, 
        csv_files, 
        config.MANDATORY_COLUMNS
    )

    # Move files with bad schemas to error folder
    if wrong_schema_csv_files:
        logger.warning(f"Found {len(wrong_schema_csv_files)} file(s) with invalid schema")
        
        # Move locally
        move_files_locally(wrong_schema_csv_files, config.bad_schema_directory)
        
        # Move in S3
        move_files_s3_to_s3(
            wrong_schema_csv_files, 
            bucket_name,
            config.s3_to_process_directory, 
            config.s3_bad_schema_directory, 
            s3_client
        )
        logger.info("Invalid schema files moved to error folder")

    # Check if any valid files remain
    if not valid_schema_csv_files:
        logger.error("No files with valid schema found")
        raise Exception("No data available with correct schema")
    
    logger.info(f"Successfully validated {len(valid_schema_csv_files)} file(s)")
    return final_df_to_process, valid_schema_csv_files


# ============================================================================
# PIPELINE STEP 5: STAGING TABLE MANAGEMENT
# ============================================================================

def update_staging_with_files(valid_schema_csv_files, db_name, staging_table_name):
    """
    Insert file records into staging table with 'START' status.
    
    Records each file being processed in the staging table for tracking.
    Status is set to 'START' and will be updated to 'COMPLETED' at end.
    
    Args:
        valid_schema_csv_files (list): List of valid file paths
        db_name (str): Database name
        staging_table_name (str): Staging table name
    """
    logger.info("Inserting file records into staging table")
    insert_staging_records(valid_schema_csv_files, db_name, staging_table_name)
    logger.info(f"Added {len(valid_schema_csv_files)} file(s) to staging table with START status")


# ============================================================================
# PIPELINE STEP 6: DATA ENRICHMENT
# ============================================================================

def load_dimension_tables(spark):
    """
    Load dimension tables from MySQL database into Spark DataFrames.
    
    Reads customer, sales_team, and store dimension tables that will be
    joined with the fact data to create enriched datasets.
    
    Args:
        spark: Spark session
        
    Returns:
        tuple: (customer_df, sales_team_df, store_df)
            Three Spark DataFrames containing dimension data
    """
    logger.info("Loading dimension tables from database")

    database_client = DatabaseReader(config.url, config.properties)

    logger.info("Loading customer dimension table")
    customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

    logger.info("Loading sales team dimension table")
    sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

    logger.info("Loading store dimension table")
    store_table_df = database_client.create_dataframe(spark, config.store_table)

    logger.info("All dimension tables loaded successfully")
    return customer_table_df, sales_team_table_df, store_table_df


def create_enriched_dataframe(final_df_to_process, customer_table_df, store_table_df, sales_team_table_df):
    """
    Join source sales data with dimension tables to create enriched dataset.
    
    Performs joins between fact (sales) data and dimension tables to add
    customer details, store information, and sales person information.
    
    Args:
        final_df_to_process: Source sales data DataFrame
        customer_table_df: Customer dimension DataFrame
        store_table_df: Store dimension DataFrame
        sales_team_table_df: Sales team dimension DataFrame
        
    Returns:
        DataFrame: Enriched dataset with all joined information
    """
    logger.info("Joining source data with dimension tables")
    
    enriched_df = dimesions_table_join(
        final_df_to_process,
        customer_table_df,
        store_table_df,
        sales_team_table_df
    )
    
    logger.info("Data enrichment completed successfully")
    return enriched_df


# ============================================================================
# PIPELINE STEP 7: CUSTOMER DATA MART CREATION
# ============================================================================

def create_and_write_customer_mart(enriched_df, bucket_name):
    """
    Create customer data mart and write to local and S3 storage.
    
    Selects customer-specific columns from enriched data and writes to:
    - Local parquet file (for backup/testing)
    - S3 parquet file (for production use)
    
    Customer data mart contains: customer details + sales transactions
    
    Args:
        enriched_df: Enriched DataFrame with all joined data
        bucket_name (str): S3 bucket name for output
        
    Returns:
        DataFrame: Customer data mart DataFrame
    """
    logger.info("=" * 60)
    logger.info("Creating Customer Data Mart")
    logger.info("=" * 60)
    
    # Select customer-relevant columns
    final_customer_data_mart_df = enriched_df.select(
        "ct.customer_id", 
        "ct.first_name", 
        "ct.last_name", 
        "ct.address",
        "ct.pincode", 
        "phone_number", 
        "sales_date", 
        "total_cost"
    )
    
    logger.info("Customer data mart schema:")
    final_customer_data_mart_df.printSchema()
    
    logger.info("Sample data from customer mart:")
    final_customer_data_mart_df.show(5)

    # Write to local parquet
    logger.info("Writing customer data mart to local storage")
    write_parquet_to_local(
        final_customer_data_mart_df, 
        config.customer_data_mart_path_local
    )

    # Write to S3 parquet
    logger.info("Writing customer data mart to S3 storage")
    write_parquet_to_s3(
        final_customer_data_mart_df, 
        bucket_name, 
        config.customer_data_mart_table_s3
    )
    
    logger.info("Customer data mart created and written successfully")
    return final_customer_data_mart_df


# ============================================================================
# PIPELINE STEP 8: SALES TEAM DATA MART CREATION
# ============================================================================

def create_and_write_sales_mart(enriched_df, bucket_name):
    """
    Create sales team data mart and write to local and S3 storage.
    
    Selects sales-team-specific columns from enriched data and writes:
    - To local parquet (non-partitioned)
    - To S3 parquet (non-partitioned)
    - To S3 parquet (partitioned by sales_month and store_id)
    
    Sales data mart contains: sales person details + transactions
    
    Args:
        enriched_df: Enriched DataFrame with all joined data
        bucket_name (str): S3 bucket name for output
        
    Returns:
        DataFrame: Sales team data mart DataFrame
    """
    logger.info("=" * 60)
    logger.info("Creating Sales Team Data Mart")
    logger.info("=" * 60)
    
    # Select sales-team-relevant columns and add sales_month
    final_sales_team_data_mart_df = enriched_df.select(
        "store_id", 
        "sales_person_id",
        "sales_person_first_name", 
        "sales_person_last_name",
        "store_manager_name", 
        "manager_id", 
        "is_manager",
        "sales_person_address", 
        "sales_person_pincode",
        "sales_date", 
        "total_cost",
        expr("SUBSTRING(sales_date,1,7) as sales_month")  # Extract YYYY-MM
    )

    logger.info("Sales team data mart schema:")
    final_sales_team_data_mart_df.printSchema()
    
    logger.info("Sample data from sales team mart:")
    final_sales_team_data_mart_df.show(5)

    # Write to local parquet (non-partitioned)
    logger.info("Writing sales team data mart to local storage")
    df_write_to_local_parquet_partitioned(
        df=final_sales_team_data_mart_df,
        file_path=config.sales_team_data_mart_path_local
    )

    # Write to S3 parquet (non-partitioned)
    logger.info("Writing sales team data mart to S3 storage")
    df_write_to_s3_parquet_partitioned(
        df=final_sales_team_data_mart_df,
        bucket=bucket_name,
        s3_key=config.sales_team_data_mart_table_s3
    )

    # Write to S3 parquet (partitioned by month and store)
    logger.info("Writing sales team data mart to S3 as partitioned dataset")
    df_write_to_s3_parquet_partitioned(
        df=final_sales_team_data_mart_df,
        bucket=bucket_name,
        s3_key=config.s3_partitioned_data_sales_team,
        partitionby=["sales_month", "store_id"]
    )
    
    logger.info("Sales team data mart created and written successfully")
    return final_sales_team_data_mart_df


# ============================================================================
# PIPELINE STEP 9: BUSINESS METRICS CALCULATION
# ============================================================================

def calculate_and_write_customer_metrics(final_customer_data_mart_df):
    """
    Calculate customer monthly purchase totals and write to MySQL.
    
    Aggregates customer purchases by month to answer:
    "How much did each customer spend each month?"
    
    Result is written to MySQL table for reporting/dashboard use.
    
    Args:
        final_customer_data_mart_df: Customer data mart DataFrame
    """
    logger.info("=" * 60)
    logger.info("Calculating Customer Monthly Purchase Metrics")
    logger.info("=" * 60)
    
    # Calculate monthly purchase totals per customer
    final_customer_monthly_purchase = customer_monthly_sales_calculate(
        final_customer_data_mart_df
    )
    
    logger.info("Customer monthly purchase schema:")
    final_customer_monthly_purchase.printSchema()
    
    logger.info("Sample customer monthly purchase data:")
    final_customer_monthly_purchase.show(5)

    # Write aggregated results to MySQL
    logger.info("Writing customer monthly purchase metrics to MySQL")
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(
        final_customer_monthly_purchase, 
        config.customer_monthly_purchase_table
    )
    
    logger.info("Customer metrics calculated and written successfully")


def calculate_and_write_sales_incentive(final_sales_team_data_mart_df):
    """
    Calculate sales team incentives and write to MySQL.
    
    Calculates monthly sales totals per salesperson and determines incentives:
    - Top performer gets 1% of their total sales as incentive
    - Other salespeople get no incentive
    
    Result is written to MySQL table for payroll processing.
    
    Args:
        final_sales_team_data_mart_df: Sales team data mart DataFrame
    """
    logger.info("=" * 60)
    logger.info("Calculating Sales Team Incentives")
    logger.info("=" * 60)
    
    # Calculate monthly sales and incentives per salesperson
    final_sales_team_incentive = sales_team_incentive_calculate(
        final_sales_team_data_mart_df
    )
    
    logger.info("Sales team incentive schema:")
    final_sales_team_incentive.printSchema()
    
    logger.info("Sample sales team incentive data:")
    final_sales_team_incentive.show(5)

    # Write incentive calculations to MySQL
    logger.info("Writing sales team incentive data to MySQL")
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(
        final_sales_team_incentive, 
        config.sales_team_incentive_table
    )
    
    logger.info("Sales incentives calculated and written successfully")


# ============================================================================
# PIPELINE STEP 10: POST-PROCESSING CLEANUP
# ============================================================================

def move_processed_files(valid_schema_csv_files, bucket_name, s3_client):
    """
    Move successfully processed files to 'processed' folders.
    
    After successful processing, files are moved from 'to_process' folder
    to 'processed' folder in both local storage and S3. This prevents
    reprocessing the same files on next run.
    
    Args:
        valid_schema_csv_files (list): List of successfully processed files
        bucket_name (str): S3 bucket name
        s3_client: Boto3 S3 client instance
    """
    logger.info("=" * 60)
    logger.info("Moving Processed Files to Archive")
    logger.info("=" * 60)
    
    # Move files in local storage
    logger.info("Moving processed files in local storage")
    move_local_to_local_folder(
        config.local_directory, 
        config.processed_directory
    )

    # Move files in S3
    logger.info("Moving processed files in S3")
    move_files_s3_to_s3(
        file_list=valid_schema_csv_files,
        bucket=config.s3_bucket_name,
        source_prefix=config.s3_to_process_directory,
        dest_prefix=config.s3_processed_directory,
        s3_client=s3_client
    )
    
    logger.info(f"Successfully moved {len(valid_schema_csv_files)} processed file(s)")


def finalize_staging_table(valid_schema_csv_files, connection):
    """
    Update staging table to mark files as 'COMPLETED'.
    
    Final step: Updates all processed files in staging table from
    'START' status to 'COMPLETED' status, indicating successful processing.
    
    Args:
        valid_schema_csv_files (list): List of processed files
        connection: MySQL database connection
    """
    logger.info("Updating staging table with COMPLETED status")
    
    update_staging_table(
        file_list=valid_schema_csv_files,
        status="COMPLETED",
        db_conn=connection
    )
    
    logger.info(f"Marked {len(valid_schema_csv_files)} file(s) as COMPLETED in staging table")


# ============================================================================
# MAIN PIPELINE ORCHESTRATION
# ============================================================================

def run_pipeline():
    """
    Main ETL pipeline orchestration function.
    
    Executes the complete sales data ETL pipeline from start to finish:
    1. Pre-processing checks (failed runs, unknown files)
    2. Extract files from S3
    3. Validate file types and schemas
    4. Enrich data with dimension tables
    5. Create customer and sales data marts
    6. Calculate business metrics (monthly purchases, incentives)
    7. Move processed files to archive
    8. Update tracking in staging table
    
    All operations are logged and database connection is properly closed
    in finally block to prevent resource leaks.
    """
    
    # ========================================================================
    # INITIALIZATION
    # ========================================================================
    logger.info("")
    logger.info("=" * 70)
    logger.info(" SALES ETL PIPELINE - STARTING")
    logger.info("=" * 70)
    logger.info("")
    
    # Initialize Spark session
    spark = get_spark_session()
    
    # Initialize AWS resources
    aws_secret_key = config.secret_key
    aws_access_key = config.access_key
    bucket_name = config.s3_bucket_name
    s3_client = get_s3_client(aws_access_key, aws_secret_key)
    
    # Initialize database resources
    db_name = config.database_name
    staging_table_name = config.staging_table_name
    connection = get_db_connection()

    try:
        # ====================================================================
        # STEP 1: PRE-PROCESSING CHECKS
        # ====================================================================
        check_previous_run_failure(connection)
        move_unknown_files()

        # ====================================================================
        # STEP 2: EXTRACT FILES FROM S3
        # ====================================================================
        local_files = extract_files_from_s3(bucket_name, s3_client)

        # ====================================================================
        # STEP 3: FILE TYPE VALIDATION
        # ====================================================================
        csv_files, wrong_files = separate_csv_and_wrong_files(local_files)
        handle_wrong_file_types(wrong_files, bucket_name, s3_client)

        


        # ====================================================================
        # STEP 4.1: FILTER ALREADY PROCESSED FILES 
        # ====================================================================


        final_csv_file = filter_already_processed_files(
            csv_files,db_name,bucket_name,config.staging_table_name,s3_client
        )



        
        
        


        # ====================================================================
        # STEP 4.2: SCHEMA VALIDATION
        # ====================================================================
        final_df_to_process, valid_schema_csv_files = validate_and_separate_files(
            spark, final_csv_file, bucket_name, s3_client
        )

        # ====================================================================
        # STEP 5: UPDATE STAGING TABLE
        # ====================================================================
        update_staging_with_files(valid_schema_csv_files, db_name, staging_table_name)

        # ====================================================================
        # STEP 6: DATA ENRICHMENT
        # ====================================================================
        customer_table_df, sales_team_table_df, store_table_df = load_dimension_tables(spark)
        enriched_df = create_enriched_dataframe(
            final_df_to_process, customer_table_df, store_table_df, sales_team_table_df
        )

        # ====================================================================
        # STEP 7: CREATE CUSTOMER DATA MART
        # ====================================================================
        final_customer_data_mart_df = create_and_write_customer_mart(enriched_df, bucket_name)

        # ====================================================================
        # STEP 8: CREATE SALES TEAM DATA MART
        # ====================================================================
        final_sales_team_data_mart_df = create_and_write_sales_mart(enriched_df, bucket_name)

        # ====================================================================
        # STEP 9: CALCULATE BUSINESS METRICS
        # ====================================================================
        calculate_and_write_customer_metrics(final_customer_data_mart_df)
        calculate_and_write_sales_incentive(final_sales_team_data_mart_df)

        logger.info("")
        logger.info("=" * 70)
        logger.info(" DATA PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info("")

        # ====================================================================
        # STEP 10: POST-PROCESSING CLEANUP
        # ====================================================================
        move_processed_files(valid_schema_csv_files, bucket_name, s3_client)
        finalize_staging_table(valid_schema_csv_files, connection)

        logger.info("")
        logger.info("=" * 70)
        logger.info(" SALES ETL PIPELINE - COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info("")

    except Exception as e:
        logger.error("")
        logger.error("=" * 70)
        logger.error(" PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error(f" Error: {str(e)}")
        logger.error("=" * 70)
        logger.error("")
        raise

    finally:
        # Always close database connection, even if pipeline fails
        if connection:
            connection.close()
            logger.info("Database connection closed")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    run_pipeline()