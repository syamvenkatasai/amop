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

# from migration_management.migration_api import MigrationScheduler
from job_sheduler import find_jobs, find_jobs_2, carrier_api_save_to_20,lambda_sync_jobs

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

    # Extract the HTTP method, path, and query string parameters from the event
    data = event.get("data")
    if not data:
        data = {}

    data = data.get("data", {})
    path = data.get("path", "")
    user = (
        data.get("username")
        or data.get("user_name")
        or data.get("user")
        or "superadmin"
    )

    # Route based on the path and method
    if path == "/find_jobs":
        result = find_jobs()
    elif path == "/carrier_api_save_to_20":
        result = carrier_api_save_to_20(data)
    elif path == "/find_jobs_2":
        result = find_jobs_2()
    elif path=="/lambda_sync_jobs":
        result=lambda_sync_jobs(data)
    elif path == "/get_module_data":
        result = get_module_data(data)
    elif path == "/get_partner_info":
        result = get_partner_info(data)
    elif path == "/get_superadmin_info":
        result = get_superadmin_info(data)
    elif path == "/update_superadmin_data":
        result = update_superadmin_data(data)
    elif path == "/update_people_data":
        result = update_people_data(data)
    elif path == "/update_partner_info":
        result = update_partner_info(data)
    elif path == "/export":
        result = export(data)
    elif path == "/get_user_module_map":
        result = get_user_module_map(data)
    elif path == "/inventory_dropdowns_data":
        result = inventory_dropdowns_data(data)
    elif path == "/update_inventory_data":
        result = update_inventory_data(data)
    elif path == "/get_status_history":
        result = get_status_history(data)
    elif path == "/customers_dropdown_data":
        result = customers_dropdown_data(data)
    elif path == "/user_data":
        result = user_data(data)
    elif path == "/people_revio_customers_list_view":
        result = people_revio_customers_list_view(data)
    elif path == "/download_people_bulk_upload_template":
        result = download_people_bulk_upload_template(data)
    elif path == "/add_people_revcustomer_dropdown_data":
        result = add_people_revcustomer_dropdown_data(data)
    elif path == "/people_bulk_import_data":
        result = people_bulk_import_data(data)
    elif path == "/submit_update_info_people_revcustomer":
        result = submit_update_info_people_revcustomer(data)
    elif path == "/reports_data_with_date_filter":
        result = reports_data_with_date_filter(data)
    elif path == "/reports_data":
        result = reports_data(data)
    else:
        result = {"flag": False, "error": "Invalid path or method"}

    return {"statusCode": 200, "body": json.dumps(result)}
