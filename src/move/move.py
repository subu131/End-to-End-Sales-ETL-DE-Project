import os
import shutil
from datetime import datetime
from src.utils.logging_config import logger


def move_files_locally(file_list, destination_dir):
    """
    Move files from their current location to destination directory.
    
    Args:
        file_list: List of file paths to move
        destination_dir: Destination directory path
    
    Returns:
        Tuple of (successful_moves, failed_moves)

 


    """
    # Create destination directory if doesn't exist
    os.makedirs(destination_dir, exist_ok=True)
    
    successful = []
    failed = []
    
    for file_path in file_list:
        try:
            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                failed.append(file_path)
                continue
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            destination_dir = f"{destination_dir}\\{timestamp}"
            os.makedirs(destination_dir, exist_ok=True)

            

            filename = os.path.basename(file_path)
            dest_path = os.path.join(destination_dir, filename)

            logger.info(f" Destination Path: {dest_path}")
            
            # # Check if file already exists in destination
            # if os.path.exists(dest_path):
            #     # Add timestamp to avoid overwriting
            #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            #     name, ext = os.path.splitext(filename)
            #     filename = f"{name}_{timestamp}{ext}"
            #     dest_path = os.path.join(destination_dir, filename)
            
            # Move the file
            shutil.move(file_path, dest_path)
            successful.append(file_path)
            logger.info(f"Moved: {filename} → {destination_dir}")
            
        except Exception as e:
            logger.error(f"Failed to move {file_path}: {e}")
            failed.append(file_path)
    
    logger.info(f"Local move complete: {len(successful)} success, {len(failed)} failed")
    return successful, failed


def move_local_to_local_folder(source_dir, destination_dir):
    """Move all files from source to destination directory with timestamp"""
    
    # Generate timestamp ONCE (outside loop)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create timestamped destination folder
    timestamped_destination = os.path.join(destination_dir, timestamp)
    
    # Create directory if doesn't exist
    os.makedirs(timestamped_destination, exist_ok=True)
    logger.info(f"Created directory: {timestamped_destination}")
    
    # Get all files in source directory
    files = os.listdir(source_dir)
    
    if not files:
        logger.info(f"No files found in {source_dir}")
        return
    
    # Move each file
    moved_count = 0
    for filename in files:
        source_path = os.path.join(source_dir, filename)
        
        # Skip directories
        if os.path.isdir(source_path):
            continue
        
        # Destination path (same timestamp folder for all files)
        destination_path = os.path.join(timestamped_destination, filename)
        
        # Move the file
        shutil.move(source_path, destination_path)
        logger.info(f"Moved: {filename} → {timestamped_destination}")
        moved_count += 1
    
    logger.info(f"Total files moved: {moved_count} to {timestamped_destination}")
    



def move_files_s3_to_s3(file_list, bucket, source_prefix, dest_prefix, s3_client):
    """
    Move files within S3 from source prefix to destination prefix.
    
    Args:
        file_list: List of file paths
        bucket: S3 bucket name
        source_prefix: Source folder prefix (e.g., 'to_process/')
        dest_prefix: Destination folder prefix (e.g., 'processed/')
        s3_client: boto3 S3 client
    
    Returns:
        Tuple of (successful_moves, failed_moves)
    """
    successful = []
    failed = []
    
    for file_path in file_list:
        try:
            # Extract just the filename
            filename = os.path.basename(file_path)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Build S3 keys
            source_key = f"{source_prefix}{filename}"
            logger.info("----------------S3 source path------")
            logger.info(f"{source_key}")

            dest_key = f"{dest_prefix}{timestamp}/{filename}"
            logger.info("----------------S3 destination path------")
            logger.info(f"{dest_key}")
            
            logger.info(f"Moving in S3: {source_key} → {dest_key}")
            
            # Copy to destination
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': source_key},
                Key=dest_key
            )
            
            # Delete from source
            s3_client.delete_object(Bucket=bucket, Key=source_key)
            
            successful.append(filename)
            logger.info(f"✓ Moved to S3: {filename}")
            
        except Exception as e:
            logger.error(f"✗ Failed to move {filename} in S3: {e}")
            failed.append(filename)
    
    logger.info(f"S3 move complete: {len(successful)} success, {len(failed)} failed")
    return successful, failed