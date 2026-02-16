import traceback
from datetime import datetime
import os
from src.utils.logging_config import logger

def write_parquet_to_local(df, file_path, mode="overwrite"):
    """Write Spark DataFrame to local parquet"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"Writing to local: {file_path}")
    df.write.format("parquet") \
        .mode(mode) \
        .save(f"{file_path}/{timestamp}")
    logger.info(f"✓ Written to: {file_path}")


def write_parquet_to_s3(df, bucket, s3_key, mode="overwrite"):
    """Write Spark DataFrame directly to S3 as parquet"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_path = f"s3a://{bucket}/{s3_key}/{timestamp}"
    logger.info(f"Writing to S3: {s3_path}")
    df.write.format("parquet") \
        .mode(mode) \
        .save(s3_path)
    logger.info(f"✓ Written to S3: {s3_path}")            


def write_parquet_to_local_partitioned(df, file_path, mode="overwrite",partitionby=[]):
    """Write Spark DataFrame to local parquet by Partitionby"""
    logger.info(f"Writing to local: {file_path}")
    df.write.format("parquet") \
        .mode(mode) \
        .save(file_path)\
        .partitionBy(partitionby[0],partitionby[1])
    logger.info(f"✓ Written to: {file_path}")


def write_parquet_to_s3_partitioned(df, bucket, s3_key, mode="overwrite",partitionby=[]):
    """Write Spark DataFrame directly to S3 as parquet by Partitionby"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_path = f"s3a://{bucket}/{s3_key}{timestamp}"
    logger.info(f"s3a://{bucket}/{s3_key}")
    df.write.format("parquet") \
        .mode(mode) \
        .save(s3_path)\
        .partitionBy(partitionby[0],partitionby[1])
    logger.info(f"✓ Written to S3: {s3_path}")      




def df_write_to_s3_parquet_partitioned(df,bucket,s3_key,mode="overwrite", partitionby=None):
    """Write Spark DataFrame to S3 as partitioned parquet"""

    if partitionby is None:
        partitionby = []

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_path = f"s3a://{bucket}/{s3_key}/{timestamp}"

    logger.info(f"Writing to s3a://{bucket}/{s3_key}")
    logger.info(f"Partition columns: {partitionby}")

    writer = df.write.format("parquet").mode(mode)

    if partitionby:
        writer = writer.partitionBy(*partitionby) 

    writer.save(s3_path)

    logger.info(f"✓ Written to S3: {s3_path}")



def df_write_to_local_parquet_partitioned(df,file_path, mode="overwrite", partitionby=None):
    """Write Spark DataFrame to local as partitioned parquet"""

    if partitionby is None:
        partitionby = []

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_path = f"{file_path}/{timestamp}"

    logger.info(f"Writing to {file_path}")
    logger.info(f"Partition columns: {partitionby}")

    writer = df.write.format("parquet").mode(mode)

    if partitionby:
        writer = writer.partitionBy(*partitionby)  

    writer.save(local_path)

    logger.info(f"✓ Written to : {file_path}")