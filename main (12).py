"""
main.py

Module for handling API requests and routing them to the appropriate function.

Functions:
- lambda_handler(event,context=None)

Author: Phaneendra Y
Date: July 22, 2024
"""

import json
import time
import os
import boto3
import pytz
from datetime import datetime  # Ensure datetime is imported
from common_utils.email_trigger import (
    send_sns_email,
    get_memory_usage,
    memory_sns,
    insert_email_audit,
)
from service_qualification import path_fun
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from common_utils.authentication_check import validate_token, Validate_Ui_token
from common_utils.permission_manager import PermissionManager

# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
logging = Logging(name="main")

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
    function_name = context.function_name if context else "sim_management"
    logging.info("Lambda function started: %s", function_name)

    # Record the start time of the function
    performance_matrix = {}
    start_time = time.time()
    performance_matrix["start_time"] = (
        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}."
        f"{int((start_time % 1) * 1000):03d}"
    )

    # Extract the HTTP method, path and query string parameters from the event
    data = event.get("data")
    if data:
        try:
            data = data
        except json.JSONDecodeError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid JSON in body"}),
            }
    else:
        data = {}

    # data=data.get('data')
    path = data.get("path")

    #sesssion Handelling
    data['session_id'] = data.get("sessionId")

    username = data.get("username")
    verified_request = True
    result = {}
    # access_token=data.get('z_access_token','')
    # ui_token=data.get('access_token','')
    # if access_token and not validate_token(access_token):
    #     result = {"message": "AD INVALID TOKEN"}
    #     result["status_code"] = 401
    #     result['flag']=False
    #     verified_request=False
    # elif ui_token and not Validate_Ui_token(username,access_token) and verified_request:
    #     result = {"message": "AD INVALID TOKEN"}
    #     result["status_code"] = 401
    #     result['flag']=False
    #     verified_request=False

    # #permission validation
    # if verified_request:
    #     permission_manager_instance = PermissionManager(db_config)
    #     result = permission_manager_instance.permission_manager(data)
    #     if isinstance(result, dict) and result.get("flag") is False:
    #         verified_request=False
    #         result['flag']=False

    # Route based on the path and method
    # Capture the hit time when the request is received
    # Capture the hit time when the request is received (same as before)
    hit_time = time.time()
    hit_time_formatted = datetime.now(pytz.timezone("Asia/Kolkata")).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    logging.info(
        f"Hit Time: {hit_time_formatted}, Request Received at: {hit_time_formatted}"
    )
    request_received_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info("Routing request for path: %s", path)
    # Route based on the path and method
    if verified_request:
        result=path_fun(path,data)

    # database = DB('AmopAlgouatDB', **db_config)
    tenant_database = data.get("db_name", "altaworx_central")
    # database Connection
    database = DB(tenant_database, **db_config)
    common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)
    if result.get("flag") == False:
        status_code = 400  # You can change this to an appropriate error code
        logging.error("Error in result: %s", result)
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
    # Capture the request completion time in IST
    request_completed_time = time.time()
    request_completed_time_formatted = datetime.now(
        pytz.timezone("Asia/Kolkata")
    ).strftime("%Y-%m-%d %H:%M:%S")

    # Calculate the time difference between hit time and request completed time
    time_taken = round(
        request_completed_time - hit_time, 4
    )  # Round to 4 decimal places
    logging.info(
        f"Request Completed: {request_completed_time_formatted}, Time Taken: {time_taken} seconds"
    )

    # Record the end time of the function
    end_time = time.time()
    performance_matrix["end_time"] = (
        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
    )
    performance_matrix["execution_time"] = f"{end_time - start_time:.4f}"
    logging.info(
        f"Request processed at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
    )

    performance_matrix["execution_time"] = f"{end_time - start_time:.4f} seconds"

    logging.info(f"Function performance_matrix: {performance_matrix} seconds")
    logging.info(
        "Lambda function execution completed in %.4f seconds", end_time - start_time
    )

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
        "started": hit_time_formatted,
        "time_taken": time_taken,
        "request_completed_time_formatted": request_completed_time_formatted,
    }
