"""
@Author : Nikhil .N
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import os
from common_utils.logging_utils import Logging
from common_utils.db_utils import DB
import requests
from requests.auth import HTTPBasicAuth
import base64
import json


logging = Logging(name="authentication_check")

db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}


def fetch_user_from_zitadel(user_id, access_token, zitadel_domain):
    """
    Fetches user details from the Zitadel Identity and Access Management system
    using the provided user ID and access token.

    Args:
        user_id (str): The unique identifier of the user in Zitadel.
        access_token (str): The access token used for authentication.
        zitadel_domain (str): The domain of the Zitadel instance.

    Returns:
        dict or str: Returns a dictionary containing user details if successful,
                      or an empty string if the request fails.
    """

    # Define the endpoint for fetching user data from Zitadel
    url = f"https://{zitadel_domain}/management/v1/users/{user_id}"

    # Set the headers for the HTTP request, including the authorization token
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Send GET request to Zitadel to fetch user data
    response = requests.get(url, headers=headers)

    # Check if the response was successful
    if response.status_code == 200:
        # If successful, return the user details as a JSON object
        return response.json()
    else:
        # If the request failed, return an empty string
        return ""


def get_client_id(token):
    """
    Retrieves the client ID from a JWT token by decoding it and fetching
    user information from Zitadel.

    Args:
        token (str): The JWT token that contains the user information.

    Returns:
        str: The client ID associated with the user, or an empty string if the
             user information cannot be retrieved.
    """

    # Retrieve the authentication token and Zitadel domain from environment variables
    access_token = os.getenv("AUTH_TOKEN", " ")
    zitadel_domain = os.getenv("zitadel_domain", "")

    # Initialize the user_id variable
    user_id = None

    try:
        # Decode the JWT token into its components (header, payload, and signature)
        header, payload, _ = token.split(".")
        decoded_payload = base64.urlsafe_b64decode(f"{payload}==").decode("utf-8")

        # Print decoded payload for debugging (you can disable it in production)
        print(f"decodded:{json.loads(decoded_payload)}")

        # Extract the 'sub' (subject) field which is the user ID
        user_id = json.loads(decoded_payload).get("sub", "")

        print(f"Decoded Information:")
        print(f"user_id: {user_id}")

    except Exception as e:
        # If decoding fails, log the error
        print(f"Failed to decode token: {e}")

    # Initialize client_id variable
    client_id = ""

    # If the user_id is found, fetch the corresponding user details from Zitadel
    if user_id:
        details = fetch_user_from_zitadel(user_id, access_token, zitadel_domain)

        # If details are found, extract the userName (client ID)
        if details:
            client_id = details["user"]["userName"]

    # Return the client ID (empty string if not found)
    return client_id


def validate_token(token):
    """
    Validates the provided token by making a request to Zitadel's introspect endpoint.
    This checks if the token is active and valid.

    Args:
        token (str): The token to validate.

    Returns:
        bool: True if the token is valid (active), False otherwise.
    """

    # Retrieve client ID and secret from environment variables
    client_id = os.getenv("CLIENT_ID", " ")
    client_secret = os.getenv("CLIENT_SECRET", " ")
    zitadel_domain = os.getenv("zitadel_domain", "")

    # Define the URL for the introspection endpoint
    introspect_url = f"https://{zitadel_domain}/oauth/v2/introspect"

    # Set headers for the introspection request
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # Prepare the payload to send the token for validation
    payload = {"token": token}

    # Make a POST request to validate the token
    response = requests.post(
        introspect_url,
        headers=headers,
        data=payload,
        auth=HTTPBasicAuth(client_id, client_secret),
    )

    # If the request is successful, check the token's active status
    if response.status_code == 200:
        token_info = response.json()
        return True
    else:
        # If validation fails, log the response status and message
        print("Failed to validate token:", response.status_code, response.text)
        return False


def Validate_Ui_token(username, token):
    """
    Validates a user interface (UI) token by comparing it with the reset token stored in the database.

    Args:
        username (str): The username of the user requesting the validation.
        token (str): The token provided by the user to validate.

    Returns:
        bool: True if the provided token matches the stored token, False otherwise.
    """
    logging.info(f"username is {username} and token is {token}")

    try:
        # Connect to the database to retrieve the stored reset token
        database = DB(os.environ["COMMON_UTILS_DATABASE"], **db_config)

        # Ensure both username and token are provided
        if not username or not token:
            raise ValueError("Username, and reset token must be provided.")

        # Fetch the reset token for the user from the database
        db_reset_token = database.get_data(
            "users", {"username": username}, ["access_token"]
        )["access_token"].to_list()

        # If no reset token is found in the database, raise an exception
        if not db_reset_token:
            raise ValueError("User not found or reset token is missing.")

        # Extract the reset token from the result (assuming there is only one token per user)
        db_reset_token = db_reset_token[0]

        logging.info(f"token is {token} and db_reset_token is {db_reset_token}")

        # Compare the provided token with the one stored in the database
        if str(token) == str(db_reset_token):
            # If the tokens match, return True
            return True
        else:
            # If the tokens do not match, return False
            return False

    except Exception as e:
        # Log the exception if any error occurs during the process
        logging.error(f"exception has an error is {e}")
        # Return False if there was any error or failure
        return False
