"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import time
import datetime
from datetime import datetime
from io import BytesIO
import requests
import base64
import json
import pandas as pd
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from common_utils.email_trigger import send_email
from common_utils.permission_manager import PermissionManager
from common_utils.daily_migration_management.migration_api import MigrationScheduler
import os

# Dictionary to store database configuration settings retrieved from environment variables.
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
logging = Logging(name="migration_api")
