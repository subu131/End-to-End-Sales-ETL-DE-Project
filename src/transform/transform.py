import os
import config
from src.utils.logging_config import *
from pyspark.sql.functions import concat_ws, col, lit
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from src.utils.utility import DatabaseWriter


def validate_df(df, mandatory_columns):
    """Check if dataframe has required columns"""
    missing = set(mandatory_columns) - set(df.columns)
    if missing:
        logger.error(f"Missing columns: {missing}")
        return False
    return True

def validate_schema(spark, file_list, mandatory_columns):
    """Complete file processing with error handling"""
    
    valid_dfs = []
    wrong_schema_csv_files = []
    valid_schema_csv_files = []
    
    for file_path in file_list:
        filename = os.path.basename(file_path)
        
        try:
            # Read CSV
            df = spark.read.format("csv")\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .load(file_path)
            
            # Validate: Check missing columns
            missing_cols = set(mandatory_columns) - set(df.columns)
            if missing_cols:
                logger.error(f"{filename}: Missing columns {missing_cols}")
                logger.info(f"{filename}: ✗ Has In Valid Schema")
                wrong_schema_csv_files.append(file_path)
                continue
            
            # Validate: Check empty dataframe
            if df.count() == 0:
                logger.error(f"{filename}: File is empty")
                wrong_schema_csv_files.append(file_path)
                continue
            
            # Handle extra columns
            extra_cols = list(set(df.columns) - set(mandatory_columns))
            if extra_cols:
                logger.warning(f"{filename}: Extra columns {extra_cols} - merging into additional_column")
                df = df.withColumn("additional_column", concat_ws(", ", *extra_cols))
            else:
                df = df.withColumn("additional_column", lit(None))
            
            valid_schema_csv_files.append(file_path)    

            # Select standard columns
            df = df.select(*mandatory_columns, "additional_column")
            
            valid_dfs.append(df)
            logger.info(f"{filename}: ✓ Has Valid Schema")
            
        except Exception as e:
            logger.error(f"{filename}: Failed to process - {e}")
            wrong_schema_csv_files.append(file_path)
    
    # Merge all valid dataframes
    # if not valid_dfs:
    #     logger.error("No valid files!")
    #     return None, wrong_schema_csv_files
    
    final_df = []
    if valid_dfs:
        final_df = valid_dfs[0]
        for df in valid_dfs[1:]:
            final_df = final_df.union(df)
    
    logger.info(f"Summary: {len(valid_dfs)} valid, {len(wrong_schema_csv_files)} errors")
    
    return final_df, wrong_schema_csv_files,valid_schema_csv_files




def dimesions_table_join(final_df_to_process,
                         customer_table_df,store_table_df,sales_team_table_df):

    
    logger.info("Joining the final_df_to_process with customer_table_df ")
    s3_customer_df_join = final_df_to_process.alias("s3_data") \
        .join(customer_table_df.alias("ct"),
              col("s3_data.customer_id") == col("ct.customer_id"),"inner") \
        .drop("product_name","price","quantity","additional_column",
              "s3_data.customer_id","customer_joining_date")

        
    logger.info("Joining the s3_customer_df_join with store_table_df ")
    s3_customer_store_df_join= s3_customer_df_join.join(store_table_df,
                             store_table_df["id"]==s3_customer_df_join["store_id"],
                             "inner")\
                        .drop("id","store_pincode","store_opening_date","reviews")

    
    logger.info("Joining the s3_customer_store_df_join with sales_team_table_df ")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(sales_team_table_df.alias("st"),
                             col("st.id")==s3_customer_store_df_join["sales_person_id"],
                             "inner")\
                .withColumn("sales_person_first_name",col("st.first_name"))\
                .withColumn("sales_person_last_name",col("st.last_name"))\
                .withColumn("sales_person_address",col("st.address"))\
                .withColumn("sales_person_pincode",col("st.pincode"))\
                .drop("id","st.first_name","st.last_name","st.address","st.pincode")
    
    return s3_customer_store_sales_df_join


#calculation for customer mart
#find out the customer total purchase every month
#write the data into MySQL customer_monthly_purchase table
def customer_monthly_sales_calculate(final_customer_data_mart_df):
    window = Window.partitionBy("customer_id","sales_date_month")
    final_customer_month_sale = final_customer_data_mart_df.withColumn("sales_date_month",
                                           substring(col("sales_date"),1,7))\
                    .withColumn("total_sales_every_month_by_each_customer",
                                sum("total_cost").over(window))\
                    .select("customer_id", concat(col("first_name"),lit(" "),col("last_name")).alias("full_name"),"address","phone_number",
                            "sales_date_month",
                            col("total_sales_every_month_by_each_customer").alias("total_sales"))\
                    .distinct()
    
    #final_customer_month_sale.show()

    return final_customer_month_sale


def sales_team_incentive_calculate(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id","sales_person_id","sales_month")
    final_sales_team_data_mart = final_sales_team_data_mart_df\
                    .withColumn("sales_month", substring(col("sales_date"),1,7))\
                    .withColumn("total_sales_every_month",sum(col("total_cost")).over(window))\
                    .select("store_id","sales_person_id",
                            concat(col("sales_person_first_name"), lit(" "),col("sales_person_last_name")).alias("full_name"),
                            "sales_month", "total_sales_every_month"
                            ).distinct()
    

    rank_windows = Window.partitionBy("store_id","sales_month").orderBy(col("total_sales_every_month").desc())
    final_sales_team_incentive = final_sales_team_data_mart\
                    .withColumn("rnk",rank().over(rank_windows))\
                    .withColumn("incentive",when( col("rnk")==1,col("total_sales_every_month")*0.01).otherwise(lit(0)) )\
                    .withColumn("incentive",round(col("incentive"),2))\
                    .select("store_id","sales_person_id","full_name",
                            "sales_month","total_sales_every_month","incentive")    
    #final_sales_team_incentive.show()

    return  final_sales_team_incentive