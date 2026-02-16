import mysql.connector
import config
import boto3
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logging_config import logger

def get_db_connection():    
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password=config.db_password,
        database = config.database_name
    )
    return connection

def get_s3_client(access_key, secret_key):
    """Create S3 client"""
    return boto3.client('s3', 
                       aws_access_key_id=access_key,
                       aws_secret_access_key=secret_key)

def get_spark_session():
    """
    Create Spark session with S3 support.
    Downloads required AWS libraries automatically.
    """
    logger.info("Creating Spark session with S3 support...")
    
    spark = SparkSession.builder \
        .appName("SalesETL") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key", config.access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", config.secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    logger.info("✓ Spark session created successfully")
    return spark

class DatabaseReader:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def create_dataframe(self,spark,table_name):
        df = spark.read.jdbc(url=self.url,
                             table=table_name,
                             properties=self.properties)
        return df
    

class DatabaseWriter:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def write_dataframe(self,df,table_name):
        try:
            print("inside write_dataframe")
            df.write.jdbc(url=self.url,
                          table=table_name,
                          mode="append",
                          properties=self.properties)
            logger.info(f"Data successfully written into {table_name} table ")
        except Exception as e:
            return {f"Message: Error occured {e}"}