"""
@Author1 : Phaneendra.Y
@Author2 : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from common_utils.authentication_check import get_client_id
import json
import os

logging = Logging(name="permission_manager")
# Dictionary to store database configuration settings retrieved from environment variables.
db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}


class PermissionManager:
    # Initializing PermissionManager
    def __init__(self, db_config):
        logging.info("Initializing PermissionManager")
        self.db_config = db_config

    def permission_manager(self, data, validation=False):
        """
        Manages access control to API endpoints based on various permissions.
        This function validates if a user has access to a particular endpoint
        by checking their role, the associated service account, and any
        relevant API state or module feature configurations.

        Args:
            data (dict): The input data containing API request details such as
                        'Partner', 'path', 'z_access_token', and other related fields.
            validation (bool): An optional parameter (currently unused) for validation.

        Returns:
            dict: A dictionary containing the access status ('flag') and an optional message
                indicating success or failure.
        """
        try:
            # Extract relevant data from the input
            Partner = data.get("Partner", "")  # Partner ID
            logging.info(f"Data received: {data}")

            # Extract and clean the API path
            o_path = data.get("path")  # Path from the request
            path = o_path.replace("/", "")  # Clean up the path (removes slashes)
            logging.info(f"Path: {path}")

            # Extract the access token used for API calls
            api_call = data.get("z_access_token", "")  # API access token
            logging.info(f"API call token: {api_call}")

            # Get the environment (e.g., SandBox, Production)
            env = os.environ.get("ENV", "SandBox")  # Default to 'SandBox' if not found
            logging.info(f"Environment: {env}")

            # Connect to the database
            common_utils_database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

            # Initialize variables for access control checks
            client_id = ""
            allowed = False
            api_allowed = True

            # If there is an API call or path is 'get_auth_token', proceed with further checks
            if api_call or path == "get_auth_token":
                # If the path is 'get_auth_token', use the client_id from the data
                if path == "get_auth_token":
                    client_id = data.get("client_id")
                else:
                    # Otherwise, retrieve the client_id from the API token
                    client_id = get_client_id(api_call)

                logging.info(f"Client ID: {client_id}")

                # Query the service_accounts table to get role and tenant based on client_id
                service_data = common_utils_database.get_data(
                    "service_accounts", {"client_id": client_id}
                )
                logging.info(f"Service account data: {service_data}")

                role = service_data["role"].to_list()[
                    0
                ]  # Extract role from service data
                tenant = service_data["tenant"].to_list()[
                    0
                ]  # Extract tenant from service data
                logging.info(f"Role: {role}, Tenant: {tenant}")

                # Query the carrier_apis table to check if the API call is allowed for the tenant
                access_carrier_apis_dataframe = common_utils_database.get_data(
                    "carrier_apis",
                    {"partner": tenant, "api_name": o_path, "env": env},
                    ["api_state"],
                )
                logging.info(
                    f"Carrier API access data: {access_carrier_apis_dataframe}"
                )

                # If API data is found, check the API state
                if not access_carrier_apis_dataframe.empty and (
                    api_call or path == "get_auth_token"
                ):
                    api_allowed = access_carrier_apis_dataframe.iloc[0]["api_state"]
                logging.info(f"API allowed: {api_allowed}")

                # Query the role_module table to get the features/modules available to the user's role
                module_features = common_utils_database.get_data(
                    "role_module", {"role": role}, ["module_features"]
                ).to_dict(orient="records")
                logging.info(f"Module features: {module_features}")

                if module_features:
                    # Parse the module features (assumed to be stored as JSON)
                    module_features = module_features[0]["module_features"]
                    logging.info(f"Module features (JSON): {module_features}")

                    try:
                        module_features = json.loads(
                            module_features
                        )  # Convert to JSON if needed
                    except:
                        pass
                    logging.info(f"Module features (parsed): {module_features}")

                    # Check if the requested path is allowed in the module features for this role
                    for module, sub_module_dict in module_features.items():
                        for sub_module, actions in sub_module_dict.items():
                            logging.info(f"Actions for sub-module: {actions}")
                            if path in actions:
                                allowed = True  # Grant access if path is in the allowed actions
                                break

            # Check AMOP API access for the specified Partner
            access_Amop_apis_dataframe = common_utils_database.get_data(
                "amop_apis",
                {"partner": Partner, "api_name": o_path, "env": env},
                ["api_state"],
            )
            if not access_Amop_apis_dataframe.empty:
                api_state_value = access_Amop_apis_dataframe.iloc[0]["api_state"]
            else:
                api_state_value = True  # Default to True if no data is found

            # If the user is not allowed or API access is not permitted, deny access
            if (not allowed) and (api_call or path == "get_auth_token"):
                message = "Access Denied: You do not have the required permissions to perform this action. Please contact your administrator."
                response = {"flag": False, "message": message}
                return response

            # If the API state is True, grant access
            if api_state_value == True:
                response = {"flag": True, "api_status": api_allowed}
                return response
            else:
                # Deny access if the API state is not allowed
                message = "Access Denied: You do not have the required permissions to perform this action. Please contact your administrator."
                response = {"flag": False, "message": message}
                return response

        except Exception as e:
            # Log any exception and deny access with an appropriate message
            logging.info(f"The exception occurred: {e}")
            message = f"Access Denied: Due to some exception. Please contact your administrator."
            response = {"flag": False, "message": message}
            return response
