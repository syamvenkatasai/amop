import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import os
import logging

# Initialize clients
s3_client = boto3.client("s3")

# Set up logging
logging.basicConfig(level=logging.DEBUG)


# OpenSearch client configuration
def create_opensearch_client():
    host = os.environ.get("OPENSEARCH_HOST")
    region = os.environ.get("AWS_REGION", "us-east-1")
    service = "es"
    credentials = boto3.Session().get_credentials()

    logging.debug(f"Creating OpenSearch client with host: {host}, region: {region}")

    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token,
    )

    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )


# Lambda handler
def lambda_handler(event, context):
    logging.debug(f"Received event: {json.dumps(event)}")
    es = create_opensearch_client()

    # Process each record in the event
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]

        # Log the received event details
        logging.debug(f"Processing file: {key} from bucket: {bucket}")

        try:
            # Get the object from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = (
                response["Body"].read().decode("utf-8")
            )  # Assuming the object is JSON formatted
            logging.debug(f"Successfully fetched object {key} from bucket {bucket}.")
        except Exception as e:
            logging.error(f"Error fetching object {key} from bucket {bucket}: {str(e)}")
            continue

        # Convert the string to JSON
        try:
            json_data = json.loads(data)
            logging.debug(f"Successfully decoded JSON data from object {key}.")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from S3 object {key}: {str(e)}")
            continue

        # Validate JSON structure (make sure it has an 'id' field)
        if "id" not in json_data:
            logging.warning(
                f"Warning: JSON object in file {key} does not contain an 'id' field. Skipping."
            )
            continue

        # Index into OpenSearch
        index_name = os.path.basename(key).split(".")[
            0
        ]  # Deriving index name from the file name
        try:
            es.index(index=index_name, id=json_data["id"], body=json_data)
            logging.debug(
                f"Successfully indexed document with ID {json_data['id']} into index {index_name}."
            )
        except Exception as e:
            logging.error(
                f"Error indexing document into OpenSearch for file {key}: {str(e)}"
            )

    return {"statusCode": 200, "body": json.dumps("Successfully processed S3 event")}
