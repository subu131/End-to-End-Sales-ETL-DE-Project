"""Extract data from S3"""
import traceback
from src.utils.logging_config import *
import os


def list_s3_files(bucket, prefix, s3_client):
    """List files in S3 bucket"""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    logger.info(f"Found {len(files)} files in S3")
    return files

def download_from_s3(bucket, s3_keys, local_dir, s3_client):
    """Download files from S3"""
    downloaded = []
    for key in s3_keys:
        filename = os.path.basename(key)        
        local_path = os.path.join(local_dir, filename)
        s3_client.download_file(bucket, key, local_path)
        downloaded.append(local_path)
        logger.info(f"Downloaded {filename} from s3_source to local")
    return downloaded


def list_files(bucket, prefix, s3_client):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket,Prefix=prefix)
            if 'Contents' in response:
                #logger.info("Total files available in folder '%s' of bucket '%s': %s", folder_path, bucket_name, response)
                files = [f"{obj['Key']}" for obj in response['Contents'] if
                         not obj['Key'].endswith('/')]
                return files
            else:
                return []
        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            logger.error("Got this error : %s",error_message)
            print(traceback_message)
            raise   