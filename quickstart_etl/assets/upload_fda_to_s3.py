import json
import time
import os
import boto3
import requests

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    EnvVar,
    asset
)
from datetime import datetime, timedelta

# Environment variables read via Dagster
FDA_API_KEY = EnvVar("FDA_API_KEY")
bucket_name = EnvVar("S3_BUCKET_NAME")
folder_name = EnvVar("S3_FOLDER_NAME")
aws_access_key = EnvVar("AWS_ACCESS_KEY")
aws_secret_key = EnvVar("AWS_SECRET_KEY")

LIMIT = 100
REQUEST_DELAY = 60 / 200
current_year = datetime.now().year
current_month = datetime.now().month
BASE_URL = "https://api.fda.gov/food/enforcement.json"


def save_data(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "a") as f:
        json.dump(data, f)
        f.write("\n")


def fetch_data(api_key, start_date, end_date, skip=1):
    params = {
        "api_key": api_key,
        "search": f"report_date:[{start_date} TO {end_date}]",
        "limit": LIMIT,
        "skip": skip
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data if "results" in data else None
    except requests.exceptions.HTTPError as err:
        print(f"Request failed with status code: {response.status_code} - {err}")
    except Exception as e:
        print(f"An error occurred: {e}")


@asset(group_name="fda_data", compute_kind="API")
def fetched_fda_data(context: AssetExecutionContext) -> MaterializeResult:
    """
    Fetch FDA food enforcement data for the previous month (or the current month if desired).
    """
    api_key = context.resources.env[FDA_API_KEY]
    now = datetime.now()
    current_date = now
    total_fetched = 0
    monthly_fetched = 0

    # For example, let's just fetch the current month’s data
    # Adjust as needed to fetch multiple months or different ranges.
    start_date = f"{current_year}{str(current_month).zfill(2)}01"
    end_of_month = (datetime(current_year, current_month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    end_date = end_of_month.strftime("%Y%m%d")

    filename = f"data/fda_data_{current_year}_{str(current_month).zfill(2)}.json"
    skip = 1

    while True:
        data = fetch_data(api_key, start_date, end_date, skip)
        if not data or not data.get("results"):
            context.log.info(f"No more data returned for {start_date} to {end_date}.")
            break

        results = data["results"]
        save_data(results, filename)
        num_records = len(results)
        monthly_fetched += num_records
        total_fetched += num_records

        if num_records < LIMIT:
            break
        skip += num_records
        context.log.info(f"Fetched {num_records} records this iteration (Monthly total: {monthly_fetched}).")
        time.sleep(REQUEST_DELAY)

    metadata = {
        "start_date": start_date,
        "end_date": end_date,
        "records_fetched": monthly_fetched,
        "file_path": filename,
        "preview": MetadataValue.md(f"Fetched {monthly_fetched} records into {filename}")
    }

    return MaterializeResult(metadata=metadata)


@asset(deps=[fetched_fda_data], group_name="fda_data", compute_kind="S3 Upload")
def uploaded_fda_data(context: AssetExecutionContext) -> MaterializeResult:
    """
    Upload the fetched FDA data file to S3 if it is larger than the previously uploaded file.
    """
    s3_bucket = context.resources.env[bucket_name]
    s3_folder = context.resources.env[folder_name]
    aws_key = context.resources.env[aws_access_key]
    aws_secret = context.resources.env[aws_secret_key]

    filename = f"data/fda_data_{current_year}_{str(current_month).zfill(2)}.json"
    s3_key = f"{s3_folder}/fda_data_{current_year}_{str(current_month).zfill(2)}.json"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name="ap-southeast-2"
    )

    new_file_size = os.path.getsize(filename)
    existing_files = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder).get('Contents', [])
    existing_file_size = next((file['Size'] for file in existing_files if filename in file['Key']), None)

    if existing_file_size is None or new_file_size > existing_file_size:
        try:
            s3_client.upload_file(Filename=filename, Bucket=s3_bucket, Key=s3_key)
            context.log.info("File uploaded successfully.")
            metadata = {
                "uploaded_file": s3_key,
                "new_file_size": new_file_size,
                "previous_file_size": existing_file_size if existing_file_size else 0
            }
        except Exception as e:
            context.log.error(f"Error uploading file: {e}")
            metadata = {
                "error": str(e)
            }
    else:
        context.log.info("Existing file is larger or equal in size; no upload performed.")
        metadata = {
            "message": "No upload performed due to size check."
        }

    return MaterializeResult(metadata=metadata)
