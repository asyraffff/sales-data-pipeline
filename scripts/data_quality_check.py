import pandas as pd
from utils.logger import setup_logging

# Setup logging
logger = setup_logging()

tagging = "check_row_count"


# Ensures transformed data has at least one row.
def check_row_count(**kwargs):

    logger.info(f"{tagging} - Running data quality checks...")

    ti = kwargs['ti']
    cleaned_file = ti.xcom_pull(task_ids='transform_task')

    try:
        df = pd.read_csv(cleaned_file)
        if len(df) == 0:
            logger.error(f"{tagging} - Transformed data has zero rows.")
            raise ValueError("Transformed data has zero rows.")
        logger.info(f"{tagging} - Row count validation passed.")
    except Exception as e:
        logger.exception(f"{tagging} - Quality check failed: %s", str(e))
        raise