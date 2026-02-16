from dotenv import load_dotenv
import os

load_dotenv()

access_key = os.getenv("aws_access_key")
secret_key = os.getenv("aws_secret_key")




# DB Connection Properties
database_name = "de_project1"
host="localhost",
user="root",
db_password=os.getenv("db_password")

# MySQL database connection properties
database_name = "de_project1"
url = f"jdbc:mysql://localhost:3306/{database_name}"

properties = {
    "user": "root",
    "password": os.getenv("db_password"),
    "driver": "com.mysql.cj.jdbc.Driver"
}



# Table & DB Names
staging_table_name = "staging_table"

customer_table_name = "customer"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

customer_monthly_purchase_table = "customer_monthly_purchase"
sales_team_incentive_table = "sales_team_incentive"



#Data Mart paths
customer_data_mart_table_s3 = "data_mart/customers_data_mart"
sales_team_data_mart_table_s3 = "data_mart/sales_team_data_mart"

customer_data_mart_path_local = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\data_mart\\customers_data_mart"
sales_team_data_mart_path_local = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\data_mart\\sales_team_data_mart"

#Partitioned Data s3 folder path

s3_partitioned_data_sales_team = "partitioned_data/sales_team"




# Local file directories
local_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\file_from_s3"
error_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\error_files\\error_files"
unknown_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\error_files\\uknown_files"
wrong_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\error_files\\wrong_files"
bad_schema_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\error_files\\bad_schema_files"
processed_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\processed_files"
already_processed_directory = "C:\\Users\\New\\Desktop\\DE_Project_1\\Data\\already_processed_files"


# S3 Prefix
s3_bucket_name = "de-project-1-s3"
s3_to_process_directory = "to_process/"
s3_error_directory = "error_files/"
s3_processed_directory = "/processed_files/"
s3_wrong_directory = "error_files/wrong_files/"
s3_bad_schema_directory = "error_files/bad_schema_files/"
s3_already_processed_directory = "already_processed_files/"


MANDATORY_COLUMNS = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]