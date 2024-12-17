"""
main.py
Module for handling API requests and routing them to the appropriate function.
Functions:
- lambda_handler(event,context=None)
Author: Nikhil N, Phaneendra Y
Date: July 22, 2024
"""

import boto3
import json
import time
from datetime import datetime
import pytz
from common_utils.db_utils import DB
import os
from common_utils.daily_migration_management.migration_api import MigrationScheduler
from common_utils.email_trigger import (
    send_sns_email,
    get_memory_usage,
    memory_sns,
    insert_email_audit,
)
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging

logging = Logging(name="main")
##database configuration
# db_config = {
#     'host': "amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user': "root",
#     'password': "AmopTeam123"}

db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}

# Initialize the SNS client
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")


def fetch_message_bodies(event):
    message_bodies = [record["body"] for record in event.get("Records", [])]
    return message_bodies


def lambda_handler(event, context):
    """
    Handles incoming API requests and routes them to the appropriate function.

    Args:
        event (dict): The incoming API request event.

    Returns:
        dict: A dictionary containing the response status code and body.

    Example:
        >>> event = {'data': {'path': '/get_modules'}}
        >>> lambda_handler(event)
        {'statusCode': 200, 'body': '{"flag": True, "modules": [...]}'}
    """

    function_name = context.function_name if context else "user_authentication"

    # Set the timezone to India Standard Time
    india_timezone = pytz.timezone("Asia/Kolkata")
    performance_matrix = {}
    # Record the start time of the function
    start_time = time.time()
    utc_time_start_time = datetime.utcfromtimestamp(start_time)
    start_time_ = utc_time_start_time.replace(tzinfo=pytz.utc).astimezone(
        india_timezone
    )
    performance_matrix["start_time"] = (
        f"{start_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((start_time % 1) * 1000):03d}"
    )
    logging.info(f"Request received at {start_time}")

    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

    # Extract the HTTP method, path, and query string parameters from the event
    print("########event", event)

    # Fetch the bodies
    message_bodies = fetch_message_bodies(event)

    print("=====================", message_bodies)
    job_name = message_bodies[0]
    migration_job = MigrationScheduler()
    result = migration_job.main_migration_func(job_name)

    if result:
        state = f"JOB -- {job_name} success"
    else:
        state = f"JOB -- {job_name} failed"

    # data = event.get('data')
    # if not data:
    #     data = {}

    # data = data.get('data', {})
    # path = data.get('path', '')
    # user = data.get('username') or data.get('user_name') or data.get('user') or 'superadmin'
    # result="Done"

    # if path == 'migration_api':
    #     # return  {"Return_dict": {"Tax_precent": "10"}}

    # else:
    #     result = {'flag': False, 'error': 'Invalid path or method'}

    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage()) + 100
    final_memory_used = get_memory_usage()
    logging.info(
        f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB"
    )
    memory_sns(memory_limit, memory_used, context)

    return {"statusCode": 200, "body": json.dumps(state)}
