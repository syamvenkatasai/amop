"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import psycopg2
import boto3
from botocore.exceptions import NoCredentialsError
import subprocess
import datetime
from time import time
import os
from logging_utils import Logging
from db_utils import DB

logging = Logging(name="prugedatabase")

# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}


def export_db_to_sql_file(host, user, password, db_name, output_file):
    try:
        # Execute pg_dump to export the database
        dump_command = f"PGPASSWORD={password} pg_dump -h {host} -U {user} {db_name} > {output_file}"
        subprocess.run(dump_command, shell=True, check=True)
        logging.debug(f"Database {db_name} exported to {output_file}")
        return True
    except psycopg2.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return False
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing pg_dump: {e}")
        return False


def upload_file_to_s3(file_name, bucket, object_name=None):
    """
    Description:The upload_file_to_s3 function uploads a specified file to an AWS S3 bucket.
    It uses the file name as the object name by default, logs the upload status
    """
    # Create an S3 client using Boto3.
    s3_client = boto3.client("s3")
    try:
        if object_name is None:
            object_name = os.path.basename(file_name)
        # Upload the file to the specified S3 bucket
        s3_client.upload_file(file_name, bucket, object_name)
        logging.debug(
            f"File {file_name} uploaded to S3 bucket {bucket} with object name {object_name}"
        )
        return True
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False


def purgedatabase():
    """
    Description:The purgedatabase function performs a series of operations to purge a database,
    including exporting the database to a SQL file, uploading the file to an S3 bucket
    """
    try:
        # Record the start time and date for the purging process.
        start_time = time()
        date_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    try:
        # Database credentials
        host = os.environ["HOST_IP"]
        user = os.environ["LOCAL_DB_USER"]
        password = os.environ["LOCAL_DB_PASSWORD"]
        db_name = os.environ["LOCAL_DB_NAME"]

        # Connect to the databas
        database = DB("amop", **db_config)

        # Output file
        output_file = os.environ["Purgedatabase_output_path"]

        # S3 bucket details
        bucket_name = os.environ["S3_bucket_name"]
        s3_object_name = os.environ["Purgedatabase_output_file"]

        # Export the database to a SQL file
        export_db_to_sql_file(host, user, password, db_name, output_file)

        # Upload the SQL file to S3
        upload_file_to_s3(output_file, bucket_name, s3_object_name)

        # End time calculation
        end_time = time()
        time_consumed = end_time - start_time

        message = "Purging database is successful."
        response = {"flag": True, "message": message}

        ## Auditing
        audit_data_user_actions = {
            "service_name": "Module_api",
            "created_date": date_started,
            "created_by": "",
            "status": str(response["flag"]),
            "time_consumed_secs": time_consumed,
            "session_id": "",
            "tenant_name": "",
            "comments": "comments",
            "module_name": "user_login",
            "request_received_at": start_time,
        }
        database.update_audit(audit_data_user_actions, "audit_user_actions")
        return response
    except Exception as e:
        message = "Purging database Failed"
        response = {"flag": True, "message": message}

        # Error handling and logging
        logging.error(f"Something went wrong and error is {e}")
        message = f"Something went wrong while getting modules. Error: {e}"

        # Log error to database
        error_data = {
            "service_name": "Module_api",
            "created_date": start_time,
            "error_message": message,
            "error_type": str(type(e)),
            "user": "",
            "session_id": "",
            "tenant_name": "",
            "comments": message,
            "module_name": "",
            "request_received_at": start_time,
        }
        database.log_error_to_db(error_data, "error_table")

        return {"flag": False, "message": message}
