
import os
from src.utils.logging_config import *
from datetime import datetime
from src.utils.utility import *


def insert_staging_records(file_list,db_name,staging_table):
    """Insert files into staging table - SAFE version"""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    insert_query = f"""
        INSERT INTO {db_name}.{staging_table}
        (file_name, file_location, created_date, status)
        VALUES (%s, %s, %s, %s)
    """
    
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for file_path in file_list:
        filename = os.path.basename(file_path)
        
        # Option 1: Convert backslashes to forward slashes
        file_location = file_path.replace('\\', '/')
        
        # Execute with parameters (automatically escapes)
        cursor.execute(insert_query, (filename, file_location, current_date, 'START'))
        logger.info(f"Inserted: {filename} -> {file_location}")
    
    connection.commit()
    cursor.close()
    connection.close()

def update_staging_table(file_list, status, db_conn):
    """Update staging table"""

    try:
        cursor = db_conn.cursor()
        for file in file_list:
            file_name = os.path.basename(file)
            query = f"UPDATE staging_table SET status='{status}' WHERE file_name='{file_name}'"
            cursor.execute(query)
        db_conn.commit()
        logger.info(f"Updated {len(file_list)} files to status {status}")    
    except Exception as e:
        logger.error(f"✗ Failed to Update Staging Table {e}")    



def check_already_processed_files(file_list, db_name, staging_table_name):
    """
    Check which files have already been processed successfully.
    
    Queries staging table to identify files with 'COMPLETED' status,
    ensuring idempotency (same file won't be processed twice).
    
    Args:
        file_list (list): List of file paths to check
        db_name (str): Database name
        staging_table_name (str): Staging table name
        
    Returns:
        tuple: (files_to_process, already_processed_files)
            - files_to_process: Files that need processing (new or failed)
            - already_processed_files: Files already completed
    """
    connection = get_db_connection()
    cursor = connection.cursor()
    
    # Extract just filenames from paths
    filenames = [os.path.basename(file) for file in file_list]
    
    if not filenames:
        logger.info("No files to check for idempotency")
        return file_list, []
    
    # Query to find already processed files
    placeholders = ', '.join(['%s'] * len(filenames))
    query = f"""
        SELECT DISTINCT file_name 
        FROM {db_name}.{staging_table_name}
        WHERE file_name IN ({placeholders})
        AND status = 'COMPLETED'
    """
    
    logger.info("Checking staging table for already processed files")
    cursor.execute(query, filenames)
    already_processed = [row[0] for row in cursor.fetchall()]
    
    cursor.close()
    connection.close()
    
    # Separate files into two lists
    files_to_process = []
    already_processed_files = []
    
    for file_path in file_list:
        filename = os.path.basename(file_path)
        if filename in already_processed:
            already_processed_files.append(file_path)
            logger.warning(f"Skipping already processed file: {filename}")
        else:
            files_to_process.append(file_path)
            logger.info(f"File will be processed: {filename}")
    
    # Summary
    logger.info("=" * 60)
    logger.info(f"Idempotency Check Results:")
    logger.info(f"  Files to process: {len(files_to_process)}")
    logger.info(f"  Already processed: {len(already_processed_files)}")
    logger.info("=" * 60)
    
    return files_to_process, already_processed_files        