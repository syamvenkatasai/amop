"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import re
import requests
import os
import pandas as pd
from datetime import time
import time
from io import BytesIO
import json
import base64
import zipfile
import io
import pytds
import base64
from pytz import timezone
import boto3
import concurrent.futures
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from common_utils.email_trigger import send_email

# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
logging = Logging(name="service_qualification")


def path_fun(path,data):

    if path == "/get_service_qualification_features":
        result = get_service_qualification_features(data)
    else:
        result = {"error": "Invalid path or method"}
        logging.warning("Invalid path or method requested: %s", path)

    return result


def get_service_qualification_features(data):
    """
    Description: The  function retrieves and organizes field mappings,headers,and module features
    based on the provided module_list, role, user, and other parameters.
    It connects to a database, fetches relevant data, categorizes fields,and
    compiles features into a structured dictionary for each module.
    """
    common_utils_database = DB("common_utils", **db_config)
    database = DB("altaworx_central", **db_config)
    parent_module_name = data.get("parent_module_name", "")
    feature_module_name = data.get("feature_module_name", "")
    user_name = data.get("username") or data.get("user_name") or data.get("user")
    tenant_name = data.get("tenant_name") or data.get("tenant")
    role = data.get("role", "")
    try:
        tenant_id = common_utils_database.get_data(
            "tenant", {"tenant_name": tenant_name}, ["id"]
        )["id"].to_list()[0]
    except Exception as e:
        logging.exception(f"Getting exception at fetching tenant id {e}")
    ret_out = {}

    try:
        final_features = []

        # Fetch all features for the 'super admin' role
        if role.lower() == "super admin":
            all_features = common_utils_database.get_data(
                "module_features",
                {
                    "module": feature_module_name,
                    "parent_module_name": parent_module_name,
                },
                ["features"],
            )["features"].to_list()
            if all_features:
                final_features = json.loads(all_features[0])
        else:
            final_features = get_features_by_feature_name(
                user_name,
                tenant_id,
                feature_module_name,
                common_utils_database,
                parent_module_name,
            )
            logging.info(
                "Fetched features for user '%s': %s", user_name, final_features
            )

    except Exception as e:
        logging.info(f"there is some error {e}")
        pass
    response = {
        "flag": True,
        "module_features": final_features,
        "message": "Features Fetched Successfully",
    }
    return response


def get_features_by_feature_name(
    user_name, tenant_id, feature_name, common_utils_database, parent_module_name
):
    # Fetch user features from the database
    user_features_raw = common_utils_database.get_data(
        "user_module_tenant_mapping",
        {"user_name": user_name, "tenant_id": tenant_id},
        ["module_features"],
    )["module_features"].to_list()

    logging.debug("Raw user features fetched: %s", user_features_raw)

    # Parse the JSON string to a dictionary
    user_features = json.loads(
        user_features_raw[0]
    )  # Assuming it's a list with one JSON string

    # Initialize a list to hold features for the specified feature name
    features_list = []

    # Check by parent_module_name first
    for module, features in user_features.items():
        if module == parent_module_name and feature_name in features:
            features_list.extend(features[feature_name])

    # If no features found by parent_module_name, check other modules
    if not features_list:
        for module, features in user_features.items():
            if feature_name in features:
                features_list.extend(features[feature_name])

    logging.info("Retrieved features: %s", features_list)
    return features_list
