"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
@Author3 : Vyshnavi
Created Date: 01-11-24
"""

import json
import time
import boto3
from datetime import datetime
from common_utils.db_utils import DB
from common_utils.email_trigger import send_email
from common_utils.logging_utils import Logging
from tenant_db_creation_script import (
    create_tenant_db,
    create_provider_main,
    remove_provider,
    create_tenant_db_service_provider,
    get_superadmin_info,
    update_superadmin_data,
)
from common_utils.email_trigger import (
    send_sns_email,
    get_memory_usage,
    memory_sns,
    insert_email_audit,
)

# Initialize the SNS client
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")
logging = Logging(name="main")


def lambda_handler(event, context):

    function_name = context.function_name if context else "user_authentication"

    performance_matrix = {}
    # Record the start time of the function
    start_time = time.time()
    performance_matrix["start_time"] = (
        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}.{int((start_time % 1) * 1000):03d}"
    )
    print(
        f"Request received at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}.{int((start_time % 1) * 1000):03d}"
    )
    # Monitor memory usage

    data = event.get("data")
    # Extract the HTTP method, path, and query string parameters from the event
    if "data" in data:
        data = data.get("data")
    path = data.get("path")

    # Route based on the path and method
    if path == "/create_tenant_db":
        result = create_tenant_db(data)
    elif path == "/create_provider_main":
        result = create_provider_main(data)
    elif path == "/remove_provider":
        result = remove_provider(data)
    elif path == "/create_tenant_db_service_provider":
        result = create_tenant_db_service_provider(data)
    elif path == "/get_superadmin_info":
        result = get_superadmin_info(data)
    elif path == "/update_superadmin_data":
        result = update_superadmin_data(data)
    else:
        result = {"error": "Invalid path or method"}

    if result.get("flag") == False:
        status_code = 400  # You can change this to an appropriate error code
    else:
        status_code = 200

    # Record the end time of the function
    end_time = time.time()
    performance_matrix["end_time"] = (
        f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
    )
    performance_matrix["execution_time"] = f"{end_time - start_time:.4f}"
    print(
        f"Request processed at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}.{int((end_time % 1) * 1000):03d}"
    )

    performance_matrix["execution_time"] = f"{end_time - start_time:.4f} seconds"

    print(f"Function performance_matrix: {performance_matrix} seconds")

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
    }
