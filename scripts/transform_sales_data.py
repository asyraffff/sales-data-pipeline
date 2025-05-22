import pandas as pd
from utils.logger import setup_logging

# Setup logging
logger = setup_logging()

tagging = "clean_and_aggregate"

def clean_and_aggregate(**kwargs):
    logger.info(f"{tagging} - Starting transformation task...")
    ti = kwargs['ti']
    
    # Pull input path explicitly
    input_path = ti.xcom_pull(task_ids='download_task', key='file_path')
    
    try:
        # Load raw data
        df = pd.read_csv(input_path, header=None)

        # Manually assign column names
        df.columns = [
            "order_id", "quantity_ordered", "price_each", "order_line_number",
            "sales", "order_date", "status", "customer_id", "month_id", "year_id",
            "product_line", "msrp", "product_code", "customer_name", "phone",
            "address_line1", "address_line2", "city", "state", "postal_code",
            "country", "territory", "contact_last_name", "contact_first_name", "deal_size"
        ]

        # Clean product_line: Title case, standardize common variants
        logger.debug(f"{tagging} - Cleaning product categories...")
        df["product_line"] = df["product_line"].str.strip().str.title()
        df["product_line"] = df["product_line"].replace({
            "Vintage Cars": "Vintage Cars",
            "vintage cars": "Vintage Cars",
            "Trains": "Trains",
            "Planes": "Planes",
            "Ships": "Ships",
            "Motorcycles": "Motorcycles",
            "Classic Cars": "Classic Cars",
            "Trucks And Buses": "Trucks & Buses",
            "trucks and buses": "Trucks & Buses"
        })

        # Convert sales to numeric and coerce errors
        logger.debug(f"{tagging} - Converting sales to numeric...")
        df["sales"] = pd.to_numeric(df["sales"], errors="coerce").fillna(0)

        # Remove extreme outliers
        logger.debug(f"{tagging} - Removing outlier sales records...")
        df = df[(df["sales"] >= 0) & (df["sales"] <= 1_000_000)]

        # Drop missing values only where needed
        logger.debug(f"{tagging} - Dropping incomplete rows...")
        df = df.dropna(subset=["product_line", "sales"])

        # Aggregate by product_line
        logger.info(f"{tagging} - Aggregating sales by product line...")
        aggregated_df = df.groupby("product_line")["sales"].sum().reset_index()
        output_path = "/tmp/sales_cleaned.csv"
        aggregated_df.to_csv(output_path, index=False)
        logger.info(f"{tagging} - Aggregated into {len(aggregated_df)} categories.")

        # Push cleaned file path via XCom
        ti.xcom_push(key='cleaned_file_path', value=output_path)

    except Exception as e:
        logger.exception(f"{tagging} - Transformation error: %s", str(e))
        raise
