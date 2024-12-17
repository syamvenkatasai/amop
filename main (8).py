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
import os
from datetime import datetime
import pytz
from common_utils.db_utils import DB
from common_utils.email_trigger import (
    send_sns_email,
    get_memory_usage,
    memory_sns,
    insert_email_audit,
)
from notification_services import path_func


from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging

logging = Logging(name="main")

# db_config = {
#     'host': "amoppostgres.c3qae66ke1lg.us-east-1.rds.amazonaws.com",
#     'port': "5432",
#     'user':"root",
#     'password':"AmopTeam123"
# }
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

    # Set the timezone to India Standard Time
    india_timezone = pytz.timezone("Asia/Kolkata")

    function_name = context.function_name if context else "notification_services"

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
    data = event.get("data")
    if not data:
        data = {}

    data = data.get("data", {})
    path = data.get("path", "")

    #sesssion Handelling
    data['session_id'] = data.get("sessionId")
    
    user = (
        data.get("username")
        or data.get("user_name")
        or data.get("user")
        or "superadmin"
    )

    # Route based on the path and method
    result = path_func(path,data)

    database = DB("altaworx_central", **db_config)

    if result.get("flag") == False:
        status_code = 400  # You can change this to an appropriate error code
        # Sending email
        result_response = send_email("Exception Mail")
        if isinstance(result, dict) and result.get("flag") is False:
            logging.info(result)
        else:
            to_emails, cc_emails, subject, body, from_email, partner_name = (
                result_response
            )
            common_utils_database.update_dict(
                "email_templates",
                {"last_email_triggered_at": request_received_at},
                {"template_name": "Exception Mail"},
            )
            query = """
                SELECT parents_module_name, sub_module_name, child_module_name, partner_name
                FROM email_templates
                WHERE template_name = 'Exception Mail'
            """

            # Execute the query and fetch the result
            email_template_data = common_utils_database.execute_query(query, True)
            if not email_template_data.empty:
                # Unpack the results
                (
                    parents_module_name,
                    sub_module_name,
                    child_module_name,
                    partner_name,
                ) = email_template_data.iloc[0]
            else:
                # If no data is found, assign default values or log an error
                parents_module_name = ""
                sub_module_name = ""
                child_module_name = ""
                partner_name = ""

            # Email audit logging
            error_message = result.get(
                "error", "Unknown error occurred"
            )  # Extracting the error message
            email_audit_data = {
                "template_name": "Exception Mail",
                "email_type": "Application",
                "partner_name": partner_name,
                "email_status": "success",
                "from_email": from_email,
                "to_email": to_emails,
                "cc_email": cc_emails,
                "comments": f"{path} - Error: {error_message}",  # Adding error message to comments
                "subject": subject,
                "action": "Email triggered",
                "body": body,
                "parents_module_name": parents_module_name,
                "sub_module_name": sub_module_name,
                "child_module_name": child_module_name,
            }
            common_utils_database.update_audit(email_audit_data, "email_audit")

    else:
        status_code = 200

    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    performance_feature = common_utils_database.get_data(
        "users", {"username": user}, ["performance_feature"]
    )["performance_feature"].to_list()[0]

    # Record the end time of the function
    end_time = time.time()
    utc_time_end_time = datetime.utcfromtimestamp(end_time)
    end_time_ = utc_time_end_time.replace(tzinfo=pytz.utc).astimezone(india_timezone)
    performance_matrix["end_time"] = (
        f"{end_time_.strftime('%Y-%m-%d %H:%M:%S')}.{int((end_time % 1) * 1000):03d}"
    )

    performance_matrix["execution_time"] = f"{end_time - start_time:.4f}"

    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage()) + 100
    final_memory_used = get_memory_usage()
    logging.info(
        f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB"
    )
    memory_sns(memory_limit, memory_used, context)

    return {
        "statusCode": status_code,
        "body": json.dumps(result),
        "performance_matrix": json.dumps(performance_matrix),
        "performance_matrix_feature": performance_feature,
    }
