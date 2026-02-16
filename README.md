
```markdown
# Sales Data ETL Pipeline

A PySpark-based ETL pipeline that processes daily sales data from AWS S3, enriches it with dimension tables, and generates business intelligence reports.

---

## 📌 Project Overview

This project automates the processing of sales transactions to answer two key business questions:
1. **How much does each customer spend every month?**
2. **Which salesperson is the top performer and what's their incentive?**

**Learning Project**: Built following [Manish Kumar's](https://www.youtube.com/@TrendyTech) Data Engineering tutorial with significant modifications and enhancements.

---

## 🎯 Business Goals

### 1. Customer Monthly Purchase Analysis
Calculate total spending per customer per month for:
- Customer retention analysis
- Spending pattern identification
- Targeted marketing campaigns

### 2. Sales Team Incentive Calculation
Identify top sales performer each month and calculate:
- Total sales per salesperson
- Top performer gets **1% incentive** of their sales
- Others get no incentive

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AWS S3 (Source)                      │
│                    to_process/sales_*.csv                   │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   Data Validation Layer                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ File Type    │  │   Schema     │  │  Extra       │       │
│  │ Validation   │→ │  Validation  │→ │  Columns     │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                PySpark Transformation Layer                 │
│                                                             │
│   Sales Data  +  Customer Table  +  Store Table             │
│                  +  Sales Team Table                        │
│                         ↓                                   │
│                  Enriched Dataset                           │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Output Layer                           │
│                                                             │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────┐   │
│  │  AWS S3        │  │   AWS S3       │  │   MySQL      │   │
│  │  Customer Mart │  │   Sales Mart   │  │   Metrics    │   │
│  │  (Parquet)     │  │   (Partitioned │  │   Tables     │   │
│  │                │  │    Parquet)    │  │              │   │
│  └────────────────┘  └────────────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow

```
1. Extract
   S3 (to_process/) → Download to Local
   
2. Validate
   ├─ CSV files only → Continue
   ├─ Non-CSV → error_files/wrong_file_types/
   ├─ Valid schema → Continue
   └─ Invalid schema → error_files/bad_schema/

3. Transform
   Sales Data + Dimension Tables (Customer, Store, Sales Team)
   → Enriched Dataset with full details

4. Load
   ├─ Customer Data Mart → S3 (Parquet)
   ├─ Sales Data Mart → S3 (Partitioned Parquet by month & store)
   ├─ Customer Monthly Purchase → MySQL
   └─ Sales Team Incentive → MySQL

5. Archive
   Processed files → processed/{timestamp}/
   Update staging table → status = 'COMPLETED'
```

---

## 🛠️ Technologies

- **Processing**: Apache Spark 3.5, PySpark
- **Storage**: AWS S3, MySQL 8.0
- **Language**: Python 3.10
- **Cloud**: AWS (S3, boto3)
- **Format**: Parquet, CSV

---

## 📁 Project Structure

```
sales-etl-pipeline/
├── src/
│   ├── main.py                        # Main pipeline orchestration
│   ├── extract/extract.py             # S3 file extraction
│   ├── transform/transform.py         # Data validation & transformation
│   ├── load/write.py                  # Write to S3 & local
│   ├── move/move.py                   # File movement operations
│   ├── staging/staging.py             # Staging table management
│   ├── utils/utility.py               # Spark, S3, DB utilities
│   └── utils/logging_config.py        # Logging config
├── config.py                          # Configuration variables
├── requirements.txt
└── README.md
```

---

## 💡 What I Learned

### Technical Skills
✅ **PySpark** - Large-scale data processing with DataFrames and SQL  
✅ **Dimensional Modeling** - Star schema design for analytics  
✅ **AWS S3 Integration** - Reading/writing data to cloud storage  
✅ **ETL Design Patterns** - Extract, Transform, Load workflows  
✅ **Data Quality** - Schema validation, error handling, idempotency  
✅ **SQL** - Complex joins, aggregations, window functions  
✅ **Fault-tolerant design** with idempotency and crash recovery

### Best Practices
✅ **Error Handling** - Segregated error folders, retry logic  
✅ **Logging** - Comprehensive pipeline tracking  
✅ **Idempotency** - Safe to re-run without duplicates  
✅ **Code Organization** - Modular, function-based structure  
✅ **Documentation** - Clear comments and docstrings  

---

## 🎨 Key Modifications from Original Tutorial

| Feature            | Original Tutorial        | My Implementation                                          |
|--------------------|--------------------------|------------------------------------------------------------|
| **Structure**      | Single file (~500 lines) | Modular: 7 separate files (extract, transform, load, etc.) |
| **Error Handling** | One `error/` folder      | Segregated: `wrong_file_types/`, `bad_schema/`, `unknown/` |
| **S3 Writes**      | Write local → upload     | **Direct S3 write** (reduced I/O)                          |
| **Partitioning**   | Hardcoded columns        | **Generic function** with `partitionby` parameter          |
| **File Archiving** | Overwrites files         | **Timestamp folders** (YYYYMMDD_HHMMSS)                    |
| **Functions**      | Inline code              | **15+ reusable functions** with single responsibility      |

### 💡 Enhancements I did

#### 1. Direct S3 Write
```python
# Before: 2 steps (write local → upload)
df.write.parquet("./local/file.parquet")
s3_client.upload_file(local_path, bucket, key)

# After: 1 step (direct write)
df.write.parquet(f"s3a://{bucket}/{key}")
```

#### 2. Flexible Partitioning
```python
# Can handle both partitioned and non-partitioned writes
write_parquet_to_s3(df, bucket, key)  # No partition
write_parquet_to_s3(df, bucket, key, partitionby=["month", "store"])  # With partition
```

#### 3. Organized Error Management
```
Before: data/error_files/ (everything mixed)

After:
data/error_files/
├── wrong_file_types/     # .txt, .xlsx, etc.
├── bad_schema/           # Missing mandatory columns
└── unknown/              # Leftover from previous failed runs
```

#### 4. Timestamp-Based Archiving
```
Before: processed/sales_data.csv (overwrites each run)

After:
processed/
├── 20240215_143022/sales_data.csv
└── 20240216_091533/sales_data.csv
```

---

## 📊 Output Data

### Customer Data Mart (S3 Parquet)
```
customer_id | first_name | last_name | sales_date | total_cost
```

### Sales Team Data Mart (S3 Partitioned Parquet)
```
Partitioned by: sales_month, store_id

Fields: sales_person_id, first_name, last_name, 
        sales_date, total_cost, store_id
```

### Customer Monthly Purchase (MySQL)
```sql
+-------------+------------+-----------+------+-------+--------------+
| customer_id | first_name | last_name | year | month | monthly_total|
+-------------+------------+-----------+------+-------+--------------+
| 1           | John       | Doe       | 2024 | 1     | 5000.00      |
| 1           | John       | Doe       | 2024 | 2     | 7500.00      |
+-------------+------------+-----------+------+-------+--------------+
```

### Sales Team Incentive (MySQL)
```sql
+----------------+------------+-----------+------+-------+-------------+-----------+
| sales_person_id| first_name | last_name | year | month | total_sales | incentive |
+----------------+------------+-----------+------+-------+-------------+-----------+
| 101            | Alice      | Johnson   | 2024 | 1     | 50000.00    | 500.00    |
| 102            | Bob        | Williams  | 2024 | 1     | 45000.00    | 0.00      |
+----------------+------------+-----------+------+-------+-------------+-----------+
```
*Top performer (Alice) gets 1% incentive*

---


## 🔮 Future Enhancements

- [ ] Apache Airflow for scheduling
- [ ] Unit tests with pytest
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] AWS Glue deployment
- [ ] Read s3 files directly and process
- [ ] Real-time processing with Kafka

---

## 🙏 Acknowledgments

Built following the Data Engineering tutorial by **[Manish Kumar](https://www.youtube.com/playlist?list=PLTsNSGeIpGnHdXyLOeZ4m6tIRPvV_2jZd)**

---


