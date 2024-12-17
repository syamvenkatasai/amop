import json
import boto3
from indexing import *
from common_utils.email_trigger import (
    send_sns_email,
    get_memory_usage,
    memory_sns,
    insert_email_audit,
)
from common_utils.logging_utils import Logging

logging = Logging(name="main")

# Initialize the SNS client
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")


def lambda_handler(event, context):
    # Extract the HTTP method, path, and query string parameters from the event
    data = event.get("data")
    print("-----------", data)
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

    data = data.get("data")
    path = data.get("path")

    # Route based on the path and method
    if path == "/perform_search":
        result = perform_search(data)
    elif path == "/reindex_all":
        result = reindex_all()
    elif path == "/fetch_dropdown":
        result = fetch_dropdown(data)
    elif path == "/export_inventory":
        result = export_inventory(data)
    elif path == "/search_export":
        result = search_export(data)
    else:
        result = {"error": "Invalid path or method"}
    # print("****",result)
    flag = result.get("flag", True)  # Default to True if 'flag' is not in result
    if flag == False:
        status_code = 400  # You can change this to an appropriate error code
    else:
        status_code = 200

    memory_limit = int(context.memory_limit_in_mb)
    memory_used = int(get_memory_usage()) + 100
    final_memory_used = get_memory_usage()
    logging.info(
        f"$$$$$$$$$$$$$$$$$$$$$$$Final Memory Used: {final_memory_used:.2f} MB"
    )
    memory_sns(memory_limit, memory_used, context)

    return {"statusCode": status_code, "body": json.dumps(result)}
