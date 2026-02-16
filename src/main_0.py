from datetime import datetime
import config
import os
from src.utils.logging_config import *
from src.utils.utility import *
from src.move.move import *
from src.extract.extract import *
from src.staging.staging import insert_staging_records, update_staging_table
from src.transform.transform import customer_monthly_sales_calculate, dimesions_table_join, sales_team_incentive_calculate, validate_df, validate_schema
from src.load.write import df_write_to_local_parquet_partitioned, df_write_to_s3_parquet_partitioned, write_parquet_to_local, write_parquet_to_local_partitioned, write_parquet_to_s3, write_parquet_to_s3_partitioned





# ---Check Previous job fail---
# Check file presence in local directory
# if file found and not in staging table move to unknown folder
# if file found and status "COMPLETE" move to unknown folder
# if file found and status "START" raise error
# Else continue



   


def run_pipeline():

    spark = get_spark_session()
    aws_secret_key = config.secret_key
    aws_access_key = config.access_key
    bucket_name = config.s3_bucket_name
    s3_client = get_s3_client(aws_access_key,aws_secret_key)
    db_name = config.database_name
    staging_table_name = config.staging_table_name
    connection = get_db_connection()




    """Check Previous job fail"""

    logger.info(f"{'*' * 8} Pipe Line Starts {'*' * 8}")
    logger.info(f"{'*' * 8} Step 1. Checking previous fail run {'*' * 8}")

    local_csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]    
    
    check_query_statement = f"select distinct file_name from "\
                            f"{config.database_name}.{config.staging_table_name} "\
                            f"where file_name IN ({str(local_csv_files)[1:-1]}) AND status = 'START'"
    
    if local_csv_files:
        logger.info(f"{'*' * 8} Connecting to DB {'*' * 8}")        
        cursor = connection.cursor()
        cursor.execute(check_query_statement)
        data = cursor.fetchall()       

        if data:
            logger.info("Your Last run was failed")
            raise Exception            
        else:
            logger.info("Unknown File found in local diretory \n\n")    
    else:
        logger.info("Last run was Successfull \n\n")


    # If any files found in local directory before processing move to unknown folder
    all_files = [file for file in os.listdir(config.local_directory)]
    
    if all_files:
        source_dir = config.local_directory
        destination_dir = config.unknown_directory
        move_local_to_local_folder(source_dir,destination_dir)

    
    """ Extract files from s3 """
    logger.info(f"{'*' * 8} Step 2. Extracting files from s3 to local {'*' * 8}")    


    
    s3_files_with_prefix = list_files(bucket_name,config.s3_to_process_directory,s3_client)
    
    if s3_files_with_prefix:
        local_files = download_from_s3(bucket_name, s3_files_with_prefix , config.local_directory , s3_client)
        logger.info(f"{'-' * 8} Downloaded s3 files to local. {'-' * 8}\n\n")
    else:
        logger.error(f"{'-' * 8} No files in s3 source. {'-' * 8}")  
        raise Exception("There is no data to process")   

    
    
    """ Seperating csv files to csv list and other files to error list """

    csv_files = []
    wrong_files = []

    if local_files:
        for file in local_files:
            if file.endswith('.csv'):
                csv_files.append(file)
            else:
                wrong_files.append(file)
    
    #move wrong files 
    
    if wrong_files:
        logger.info("--------Moving wrong files-------")
        move_files_locally(wrong_files,config.wrong_directory)
        move_files_s3_to_s3(wrong_files,bucket_name,config.s3_to_process_directory,config.s3_wrong_directory,s3_client)      
    
    
                   
    """ 
    Validate Schema of csv_files

    Args: 
        Validate each csv file with mandatory columns
        Handles extra column files
    
    Returns:
        Right Schema files
        Wrong Schema files
        Union of Right files df      
    """  
    logger.info("Validating Schema of csv_files")
    final_df_to_process, wrong_schema_csv_files ,valid_schema_csv_files = validate_schema(spark,csv_files,config.MANDATORY_COLUMNS)

    logger.info("Moving wrong_schema_csv_files on both local and s3")
    move_files_locally(wrong_schema_csv_files,config.bad_schema_directory,)
    move_files_s3_to_s3(wrong_schema_csv_files,bucket_name,config.s3_to_process_directory,config.s3_bad_schema_directory,s3_client)

    print(wrong_files)
    print(wrong_schema_csv_files)

    logger.info(" Adding Status to staging table.....")


    if not valid_schema_csv_files:
        logger.error("No files to process")
        raise Exception("No data available with correct files")
    
    insert_staging_records(valid_schema_csv_files,db_name,staging_table_name)

    logger.info(" Adding Status to staging table done.")



    logger.info(" Enriching Data.")

    database_client = DatabaseReader(config.url,config.properties)

    logger.info("*********** Loading Customer table to customer_table_df *************")
    customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)

    # logger.info("*********** Loading Product table to product_table_df *************")
    # product_table_df = database_client.create_dataframe(spark,config.product_table)

    # logger.info("*********** Loading Product Staging table to product_staging_table_df *************")
    # product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)

    logger.info("*********** Loading Sales Team table to sales_team_table_df *************")
    sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

    logger.info("*********** Loading Store table to store_table_df *************")
    store_table_df = database_client.create_dataframe(spark,config.store_table)

    s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)    



    """Writing Enriched customer data to mart (Customer details + sales date , Total cost )"""

    logger.info("*********** Write data to Customer Data Mart *************")
    final_customer_data_mart_df = s3_customer_store_sales_df_join\
                                    .select("ct.customer_id","ct.first_name","ct.last_name","ct.address",
                                            "ct.pincode", "phone_number",
                                            "sales_date","total_cost")
    logger.info("*********** Final Data for Customer Data Mart *************")  
    final_customer_data_mart_df.show()

    logger.info("Writing final_customer_data_mart_df to local in parquet format")
    write_parquet_to_local(final_customer_data_mart_df,config.customer_data_mart_path_local)

    logger.info("Writing final_customer_data_mart_df to s3 in parquet format")
    write_parquet_to_s3(final_customer_data_mart_df,bucket_name,config.customer_data_mart_table_s3)


    """Writing Enriched sales team data to mart """


    logger.info("*********** Write data to Sales Team Data Mart *************")
    final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                                .select("store_id", "sales_person_id",
                                        "sales_person_first_name","sales_person_last_name",
                                        "store_manager_name","manager_id","is_manager",
                                        "sales_person_address","sales_person_pincode",
                                        "sales_date","total_cost",
                                        expr("SUBSTRING(sales_date,1,7) as sales_month")
                                        )

    logger.info("*********** Final DataFrame for Sales Team Data Mart *************")
    final_sales_team_data_mart_df.show()

    logger.info("Writing final_sales_team_data_mart_df to local in parquet format")
    df_write_to_local_parquet_partitioned(df = final_sales_team_data_mart_df,
                                          file_path=config.sales_team_data_mart_path_local)

    logger.info("Writing final_sales_team_data_mart_df to s3 in parquet format")
    df_write_to_s3_parquet_partitioned(df = final_sales_team_data_mart_df,
                                       bucket = bucket_name,
                                       s3_key = config.sales_team_data_mart_table_s3)


    logger.info("*********** Write Sales Team DataFrame to S3 as partitioned parquet*************")
   

    df_write_to_s3_parquet_partitioned(df=final_sales_team_data_mart_df,
                                    bucket=bucket_name,
                                    s3_key=config.s3_partitioned_data_sales_team,
                                    partitionby=["sales_month","store_id"]
                                    )




    """ Calculating Cutomer monthly purchase """

    final_customer_monthly_purchase =  customer_monthly_sales_calculate(final_customer_data_mart_df)
    final_customer_monthly_purchase.printSchema()

    logger.info("Writing final cutomer_monthly_purchase to MySQL table  ")    
    
    db_writer = DatabaseWriter(url=config.url,properties=config.properties)
    db_writer.write_dataframe(final_customer_monthly_purchase,config.customer_monthly_purchase_table)


    """ Calculating Sales Team Incentive """

    final_sales_team_incentive = sales_team_incentive_calculate(final_sales_team_data_mart_df)
    final_sales_team_incentive.printSchema()

    logger.info("final sales team incentive table inside main file  ")
    #final_sales_team_incentive.show()

    logger.info("Writing final sales team incentive to MySQL table  ") 
        
    db_writer.write_dataframe(final_sales_team_incentive,config.sales_team_incentive_table)



    logger.info(" Data Processing Done ") 


    logger.info(" Moving files from source to processed folder local")

    move_local_to_local_folder(config.local_directory,config.processed_directory)

    logger.info(" Moving files from source to processed folder s3")   

    move_files_s3_to_s3(file_list=valid_schema_csv_files,
                        bucket=config.s3_bucket_name,
                        source_prefix=config.s3_to_process_directory,
                        dest_prefix=config.s3_processed_directory,
                        s3_client=s3_client
                        )
    

    logger.info(" Update Staging Table Status ")   

    update_staging_table(file_list=valid_schema_csv_files,
                         status="COMPLETED",
                         db_conn=connection
                         )

    

    
    connection.close()













if __name__ == "__main__":
    run_pipeline()    