## Sales Data ETL Pipeline with Airflow and Python

This is part of my technical assessment for the Data Engineer role at Yayasan Peneraju. This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process daily sales data. The pipeline extracts raw CSV files from an S3 bucket, cleans and aggregates the data, performs quality checks, and loads the transformed data into a PostgreSQL database.

---

### Overview

<img width="662" alt="image" src="https://github.com/user-attachments/assets/68fc3133-beef-4745-b974-267832e4d30e" />

The ETL pipeline automates the following tasks:
- **Extract**: Downloads the latest daily sales data CSV file from an S3 bucket.
- **Transform**: Cleans and aggregates the data by product category, ensuring consistency and removing invalid records.
- **Quality Check**: Validates that the transformed data contains at least one row before loading.
- **Load**: Stores the transformed data in a PostgreSQL database.

The pipeline is orchestrated using Apache Airflow, which ensures reliable scheduling and task dependencies. It is containerized using Docker for easy deployment and reproducibility.

---

### Project Structure

The project is organized as follows:

```
sales-data-pipeline/
├── dags/
│   └── sales_etl_dag.py          # Airflow DAG definition
├── data/
│   └── sales_data_21_May_2025.csv # Sample raw data file
├── scripts/
│   ├── extract_sales_data.py     # Script to download data from S3
│   ├── transform_sales_data.py   # Script to clean and aggregate data
│   ├── load_to_postgres.py       # Script to load data into PostgreSQL
│   ├── data_quality_check.py     # Script for data quality checks
|   └── __init__.py               # Package initialization
│   └── utils/
│       ├── logger.py             # Logging utilities
│       └── __init__.py           # Package initialization
├── tests/
│   └── test_transform_sales_data.py # Unit tests for transformation logic
├── docker-compose.yml            # Docker configuration for Airflow
├── .env                          # Environment variables (e.g., AWS credentials, DB credentials)
├── README.md                     # Documentation
└── requirements.txt              # Python dependencies
```

---

### **Key Components**

#### 1. **Airflow DAG (`dags/sales_etl_dag.py`)**
- **Purpose**: Defines the workflow of the ETL pipeline.
- **Tasks**:
  - `extract_task`: Downloads the latest sales data from S3.
  - `transform_task`: Cleans and aggregates the data.
  - `quality_check_task`: Validates the transformed data.
  - `load_task`: Loads the cleaned data into PostgreSQL.
- **Schedule**: Runs daily at midnight (`@daily`).

#### 2. **Data Extraction (`scripts/extract_sales_data.py`)**
- **Purpose**: Downloads the latest sales data CSV file from an S3 bucket.
- **Features**:
  - Uses `boto3` to interact with S3.
  - Logs the download process and handles errors gracefully.
  - Stores the downloaded file locally.

#### 3. **Data Transformation (`scripts/transform_sales_data.py`)**
- **Purpose**: Cleans and aggregates the raw sales data.
- **Steps**:
  - Assigns column names to the raw data.
  - Standardizes product categories (e.g., "Vintage Cars", "Trains").
  - Converts sales values to numeric format and removes invalid entries.
  - Removes outliers in sales data.
  - Aggregates total sales by product category.
- **Output**: A cleaned CSV file stored in `/tmp`.

#### 4. **Data Loading (`scripts/load_to_postgres.py`)**
- **Purpose**: Loads the transformed data into a PostgreSQL database.
- **Features**:
  - Connects to PostgreSQL using environment variables.
  - Creates the `sales_by_category` table if it doesn't exist.
  - Uses batch inserts for efficient loading.
  - Logs the loading process and handles errors.

#### 5. **Data Quality Checks (`scripts/data_quality_check.py`)**
- **Purpose**: Ensures the transformed data is valid.
- **Check**: Verifies that the transformed data contains at least one row.
- **Action**: Raises an error if the row count is zero.

#### 6. **Logging (`utils/logger.py`)**
- **Purpose**: Provides consistent logging across all scripts.
- **Features**:
  - Logs to both console and timestamped log files in the `logs/` directory.
  - Includes detailed error handling and debugging information.

#### 7. **Testing (`tests/test_transform_sales_data.py`)**
- **Purpose**: Ensures the transformation logic works as expected.
- **Tests**:
  - Validates data cleaning and aggregation.
  - Checks for proper handling of missing or invalid values.

---

### **Raw Data Description**
I found this sample data from [Kaggle](https://www.kaggle.com/datasets/kyanyoga/sample-sales-data/data) and opted to use it in this assessment. The raw sales data is exported daily from an ERP system and stored in an S3 bucket. The data is in CSV format and includes the following columns:

| Column Name               | Description                                      |
|---------------------------|--------------------------------------------------|
| `order_id`                | Unique identifier for each order.               |
| `quantity_ordered`        | Quantity of items ordered.                      |
| `price_each`              | Price per item.                                 |
| `order_line_number`       | Line number within the order.                   |
| `sales`                   | Total sales amount for the order line.          |
| `order_date`              | Date of the order.                              |
| `status`                  | Status of the order (e.g., completed, pending). |
| `customer_id`             | Identifier for the customer.                    |
| `month_id`                | Month ID for the order.                         |
| `year_id`                 | Year ID for the order.                          |
| `product_line`            | Category of the product (e.g., Vintage Cars).   |
| `msrp`                    | Manufacturer's suggested retail price.         |
| `product_code`            | Code for the product.                           |
| `customer_name`           | Name of the customer.                           |
| `phone`                   | Customer's phone number.                        |
| `address_line1`           | First line of the customer's address.          |
| `address_line2`           | Second line of the customer's address.         |
| `city`                    | City of the customer's address.                 |
| `state`                   | State of the customer's address.                |
| `postal_code`             | Postal code of the customer's address.         |
| `country`                 | Country of the customer's address.              |
| `territory`               | Sales territory.                                |
| `contact_last_name`       | Last name of the contact person.               |
| `contact_first_name`      | First name of the contact person.              |
| `deal_size`               | Size of the deal (e.g., small, medium, large).  |

#### **Sample Raw Data**
```csv
order_id,quantity_ordered,price_each,order_line_number,sales,order_date,status,customer_id,month_id,year_id,product_line,msrp,product_code,customer_name,phone,address_line1,address_line2,city,state,postal_code,country,territory,contact_last_name,contact_first_name,deal_size
10001,2,500,1,1000,2025-05-21,Completed,101,5,2025,Vintage Cars,500,P1001,John Doe,+1234567890,123 Main St,,Anytown,CA,90210,USA,North America,Doe,John,Large
10002,1,200,1,200,2025-05-21,Completed,102,5,2025,Trains,200,P1002,Jane Smith,+1987654321,456 Elm St,,Othertown,CA,90211,USA,North America,Smith,Jane,Small
```

---

### **Transformed Data Description**
The transformed data is aggregated by `product_line` and includes the following columns:

| Column Name     | Description                                      |
|-----------------|--------------------------------------------------|
| `category`      | Product category (e.g., Vintage Cars, Trains).   |
| `total_sales`   | Total sales amount for the category.             |

#### **Sample Transformed Data**
```csv
category,total_sales
Vintage Cars,1200
Trains,200
```

#### **PostgreSQL Table Schema**
The transformed data is loaded into the `sales_by_category` table in PostgreSQL.

```sql
CREATE TABLE IF NOT EXISTS sales_by_category (
    id SERIAL PRIMARY KEY,
    category VARCHAR(255) NOT NULL,
    total_sales NUMERIC NOT NULL
);
```

#### Example Transformed Data in PostgreSQL:

| id | category         | total_sales |
|----|------------------|-------------|
| 1  | Classic Cars     | 120000      |
| 2  | Trains           | 80000       |
| 3  | Vintage Cars     | 95000       |
| 4  | Motorcycles      | 70000       |

> ✅ This table gives a clear, clean summary of daily sales by product line, suitable for reporting and analytics.

---

### Setup Instructions

#### Prerequisites
- **Docker**: Ensure Docker is installed on your machine.
- **AWS Credentials**: Configure AWS access keys in the `.env` file or environment variables.
- **PostgreSQL**: Set up a PostgreSQL instance locally or remotely. Update the database connection details in the `.env` file.

#### Steps to Run the Pipeline

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/sales-data-pipeline.git
   cd sales-data-pipeline
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**
   Create a `.env` file with the following variables:
   ```env
   AIRFLOW_UID=50000
   S3_BUCKET_NAME=your-s3-bucket-name
   S3_PREFIX=sales/
   AWS_ACCESS_KEY_ID=your-access-key-id
   AWS_SECRET_ACCESS_KEY=your-secret-access-key
   PG_HOST=localhost
   PG_DATABASE=airflow
   PG_USER=airflow
   PG_PASSWORD=airflow
   PG_PORT=5432
   ```

4. **Start Airflow Using Docker**
   ```bash
   docker-compose up -d
   ```

5. **Access Airflow UI**
   Open http://localhost:8080 in your browser. Use the default credentials:
   ```
   Username: airflow
   Password: airflow
   ```
   ![WhatsApp Image 2025-05-22 at 10 15 57_3f3c05ed](https://github.com/user-attachments/assets/74aeefd1-0c61-4a64-b347-133452537365)
   > your airflow dags should look like this


7. **Trigger the DAG**
   - Navigate to the "DAGs" tab in the Airflow UI.
   - Locate the `sales_etl_pipeline` DAG.
   - Trigger the DAG manually or wait for the scheduled run (`@daily`).

---

### Key Design Choices and Assumptions

#### Design Choices
1. **Airflow for Orchestration**:
   - Airflow is used to define the DAG and manage task dependencies, retries, and scheduling.
2. **Docker for Containerization**:
   - The pipeline is containerized using Docker for ease of deployment and reproducibility.
3. **Logging**:
   - A centralized logging mechanism is implemented using Python's `logging` module, with logs stored in timestamped files under the `logs/` directory.
4. **Error Handling**:
   - Each script includes robust error handling with detailed logging and exception propagation.
5. **Batch Inserts**:
   - PostgreSQL inserts use batch processing (`execute_batch`) for improved performance.

#### Assumptions
1. **S3 Bucket Access**:
   - The S3 bucket is accessible with the provided AWS credentials.
2. **PostgreSQL Connection**:
   - The PostgreSQL database is reachable with the specified connection details.
3. **File Format**:
   - The input CSV file has a consistent structure and column names.

---

### Running the Pipeline

1. **Start Airflow**:
   ```bash
   docker-compose up -d
   ```

2. **Access Airflow UI**:
   - Open http://localhost:8080.
   - Trigger the `sales_etl_pipeline` DAG manually or wait for the scheduled run.

3. **Monitor Logs**:
   - Logs are stored in the `logs/` directory.
   - Real-time logs can also be viewed in the Airflow UI.

4. **Verify Results**:
   - Check the `sales_by_category` table in PostgreSQL to ensure data is loaded correctly.
   - Review the DAG status in the Airflow UI to confirm successful execution.

---

### Testing
Run unit tests for the transformation logic:
```bash
python -m pytest tests/test_transform_sales_data.py
```
