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
from daily_migration_management.migration_api import MigrationScheduler

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


def send_sns_email(subject, message):
    """Send an email via SNS when memory or CPU limits are breached."""
    response = sns.publish(
        TopicArn="arn:aws:sns:us-east-1:008971638399:custom-alert",
        Message=message,
        Subject=subject,
    )
    logging.info("SNS publish response:", response)
    return response


def get_lambda_metrics(metric_name, function_name):
    """Fetch specific Lambda CloudWatch metrics (invocations or throttles)."""
    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName=metric_name,
        Dimensions=[{"Name": "FunctionName", "Value": function_name}],
        StartTime=time.time() - 300,  # Last 5 minutes
        EndTime=time.time(),
        Period=300,  # 5-minute period
        Statistics=["Sum"],
    )
    return response["Datapoints"][0]["Sum"] if response["Datapoints"] else 0


def get_api_gateway_metrics(metric_name, api_id):
    """Fetch specific API Gateway CloudWatch metrics."""
    response = cloudwatch.get_metric_statistics(
        Namespace="AWS/ApiGateway",
        MetricName=metric_name,
        Dimensions=[{"Name": "ApiId", "Value": api_id}],
        StartTime=time.time() - 300,  # Last 5 minutes
        EndTime=time.time(),
        Period=300,  # 5-minute period
        Statistics=["Sum"],
    )
    return response["Datapoints"][0]["Sum"] if response["Datapoints"] else 0


def get_memory_usage():
    """Get the current memory usage in MB."""
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss / (1024 * 1024)


# Check if memory usage exceeds 80% of the limit
def memory_sns(memory_limit, memory_used, context):
    if memory_used > 0.8 * memory_limit:
        subject = "Lambda Memory Usage Alert"
        message = f"Warning: Memory usage has exceeded allocated limit.\n\nDetails:\nMemory Used: {memory_used} MB\nMemory Limit: {memory_limit} MB\nFunction: {context.function_name}\nRequest ID: {context.aws_request_id}"
        send_sns_email(subject, message)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at)
        logging.info("###mail sent")


def insert_email_audit(subject, message, request_received_at):
    """Insert email audit data into the database."""
    try:
        # Connect to the database
        db = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

        # Email audit data
        email_audit_data = {
            "template_name": "lambda ram exceeded",
            "email_type": "AWS",
            "partner_name": "Altaworx",
            "email_status": "success",
            "from_email": "sns.amazon.com",
            "to_email": "AWS sns mails",
            # "cc_email": cc_emails,
            "comments": "Lambda memory error",
            "subject": subject,
            # "body": body,
            "role": "admin",
            # "action_performed": action_performed
        }
        db.update_audit(email_audit_data, "email_audit")

        # Auditing data
        email_auditing_data = {
            "service_name": "Module_management",
            "created_date": datetime.now(),  # Use current timestamp
            "created_by": "AWS",
            "status": str(True),  # Adjust based on your logic
            # "time_consumed_secs": time_consumed,
            # "session_id": session_id,
            "tenant_name": "",  # Add tenant name if applicable
            # "comments": message,
            "module_name": "user_login",
            # "request_received_at": request_received_at
        }
        db.update_audit(email_auditing_data, "email_auditing")

    except Exception as e:
        logging.exception(f"Error inserting email audit data: {e}")


def check_throttling_and_alert(function_name, performance_matrix):
    """
    Check for Lambda throttling and send an alert if throttling occurs.
    Additionally, record the throttling information in the performance matrix.
    """
    throttles = get_lambda_metrics("Throttles", function_name)
    logging.info(f"Throttles: {throttles}")

    # Log throttling data in performance matrix
    performance_matrix["throttles"] = throttles

    # Alert if throttling occurs
    if throttles > 0:
        subject = f"Lambda Throttling Alert for {function_name}"
        message = (
            f"Warning: Lambda throttling detected.\n\nDetails:\n"
            f"Throttles: {throttles}\nFunction: {function_name}"
        )
        send_sns_email(subject, message, context)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at, context)
        logging.info("###mail sent")
        logging.info(f"Throttling alert sent for {throttles} throttling events.")


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

    # Monitor API Gateway errors (4XX and 5XX)
    api_id = "v1djztyfcg"  # Replace with your actual API Gateway ID
    client_errors = get_api_gateway_metrics("4XXError", api_id)
    server_errors = get_api_gateway_metrics("5XXError", api_id)

    if client_errors > 0:
        subject = "API Gateway Client Error Alert"
        message = f"Warning: API Gateway encountered client errors (4XX).\n\nDetails:\nClient Errors: {client_errors}\nAPI: {api_id}"
        send_sns_email(subject, message, context)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at, context)
        logging.info("###mail sent")

    if server_errors > 0:
        subject = "API Gateway Server Error Alert"
        message = f"Warning: API Gateway encountered server errors (5XX).\n\nDetails:\nServer Errors: {server_errors}\nAPI: {api_id}"
        send_sns_email(subject, message, context)
        request_received_at = datetime.now()
        insert_email_audit(subject, message, request_received_at, context)
        logging.info("###mail sent")

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

    check_throttling_and_alert(function_name, performance_matrix)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

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

    if path == "migration_api":
        # return  {"Return_dict": {"Tax_precent": "10"}}
        migration_job = MigrationScheduler()
        return migration_job.main_migration_func(data)
    else:
        result = {"flag": False, "error": "Invalid path or method"}

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
                "body": body,
                "action": "Email triggered",
                "parents_module_name": parents_module_name,
                "sub_module_name": sub_module_name,
                "child_module_name": child_module_name,
            }
            common_utils_database.update_audit(email_audit_data, "email_audit")

    else:
        status_code = 200
    database = DB("altaworx_central", **db_config)
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

    return {
        "statusCode": status_code,
        "body": json.dumps(result),
        "performance_matrix": json.dumps(performance_matrix),
        "performance_matrix_feature": performance_feature,
    }
