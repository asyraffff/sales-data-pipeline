import boto3
import os
from utils.logger import setup_logging
from dotenv import load_dotenv
import pandas as pd
import io

# Load environment variables
load_dotenv()

# Setup logging
logger = setup_logging()

tagging = "download_latest_file"

# Downloads latest CSV file from S3
# Returns path to local file
def download_latest_file(**kwargs):

    logger.info(f"{tagging} - Starting S3 extraction task...")

    try:
        s3_bucket = os.getenv("S3_BUCKET_NAME")
        s3_prefix = os.getenv("S3_PREFIX")

        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        # sort based on latest file
        files = sorted(response.get('Contents', []), key=lambda x: x['LastModified'], reverse=True)

        if not files:
            logger.error(f"{tagging} - No files found in S3 bucket.")
            raise FileNotFoundError(f"{tagging} - No files found in specified S3 bucket.")

        latest_file = files[0]['Key']
        logger.info(f"{tagging} - Downloading latest file: {latest_file}")

        csv_obj = s3.get_object(Bucket=s3_bucket, Key=latest_file)
        body = csv_obj['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(body))

        output_path = f"/data/{latest_file}.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"{tagging} - Downloaded {len(df)} rows from S3.")

        # Push the DataFrame to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='file_path', value=output_path)

    except Exception as e:
        logger.exception(f"{tagging} - Error during S3 extraction: %s", str(e))
        raise