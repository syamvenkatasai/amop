"""
@Author1 : Phaneendra.Y
Created Date: 01-11-24
"""

# Importing the necessary Libraries
import os
import pandas as pd
from common_utils.db_utils import DB
from common_utils.logging_utils import Logging
from datetime import datetime, timedelta

db_config = {
    "host": os.environ["HOST"],
    "port": os.environ["PORT"],
    "user": os.environ["USER"],
    "password": os.environ["PASSWORD"],
}
logging = Logging(name="dashboard")


#######Dashboard code
def path_fun(path, data):

    if path == "/get_service_providers":
        result = get_service_providers(data)
    elif path == "/count_of_service_provider":
        result = count_of_service_provider(data)
    elif path == "/count_of_active_sims":
        result = count_of_active_sims(data)
    elif path == "/count_of_pending_sim_activations":
        result = count_of_pending_sim_activations(data)
    elif path == "/device_status_chart":
        result = device_status_chart(data)
    elif path == "/activated_vs_deactivated_pie_chart":
        result = activated_vs_deactivated_pie_chart(data)
    elif path == "/service_provider_change_request_stack_bar":
        result = service_provider_change_request_stack_bar(data)
    elif path == "/count_of_active_customers":
        result = count_of_active_customers(data)
    elif path == "/rev_assurance_record_discrepancy_card":
        result = rev_assurance_record_discrepancy_card(data)
    elif path == "/daily_sync_card":
        result = daily_sync_card(data)
    elif path == "/live_sessions_table":
        result = live_sessions_table(data)
    elif path == "/get_compare_cards":
        result = get_compare_cards(data)
    else:
        result = {"error": "Invalid path or method"}
        logging.warning("Invalid path or method requested: %s", path)

    return result


def get_compare_cards(data):
    """
    Given a year and a list of months, returns a dictionary with month names (or numbers) as keys
    and a dictionary with 'start_date' and 'end_date' for each month.

    Parameters:
    year (int): The year for which the start and end dates are required.
    months (list): A list of month names (e.g., ['January', 'March', 'November']) or month numbers (e.g., [1, 3, 11]).

    Returns:
    dict: A dictionary where each key is a month and the value is a dictionary with 'start_date' and 'end_date'.
    """
    year = int(data["year"])  # Convert year to integer
    months = data["months"]
    path = data["chart_path"]

    month_dates = {}
    try:
        for month in months:
            acc_month = month
            # Convert month name to number if it is a string (e.g., 'January' -> 1)
            if isinstance(month, str):
                month = datetime.strptime(month, "%B").month

            # Construct the start date for the month (first day of the month)
            start_date = f"{year}-{month:02d}-01"
            # Get the last day of the month (end date)
            next_month = month % 12 + 1  # Next month
            next_month_year = (
                year if next_month > 1 else year + 1
            )  # Adjust year if next month is January

            end_date = f"{next_month_year}-{next_month:02d}-01"

            # Subtract one day from the next month's first day to get the last day of the current month
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)
            end_date = end_date_obj.strftime("%Y-%m-%d")
            data.update({"start_date": start_date, "end_date": end_date})

            # You can add your function to process the data here (if needed)
            result = call_funtion(path, data)
            result.pop("flag")
            # Store both start and end dates
            month_dates[acc_month] = result

        return {"flag": True, "month_dates": month_dates}
    except Exception as e:
        logging.warning("error here: %s", e)
        return {"flag": False, "month_dates": month_dates}


def get_service_providers(data):
    logging.info("Request Recieved")
    ## database connection
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    try:
        service_providers = database.get_data(
            "serviceprovider", {"is_active": True}, ["service_provider_name"]
        )["service_provider_name"].to_list()
        logging.info("Query defined to fetch active service providers.")
        service_providers.append("All Service Providers")
        # Create and return the response
        response = {"flag": True, "service_providers": service_providers}
        logging.debug("Response prepared successfully: %s", response)
        return response
    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        return {"flag": False, "service_providers": []}


def count_of_service_provider(data):
    """
    Fetches the total number of service_provider's that got triggered from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering service provider's (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering service provider's (format: YYYY-MM-DD).
    Returns:
        dict: containing the status of total_emails_count and the data card information.
    """
    # service_provider = data.get('service_provider')
    # start_date = data.get('start_date')
    # end_date = data.get('end_date')
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    query = """
        SELECT COUNT(*)
        FROM serviceprovider where is_active=True
    """
    try:
        # Execute query with parameters
        res = database.execute_query(query, True)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            total_service_providers = int(
                res.iloc[0, 0]
            )  # Convert to standard Python int
        else:
            total_service_providers = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "No: of Service Providers",
                "chart_type": "data",
                "data": total_service_providers,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except Exception as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching total_service_providers",
        }
    return response



def count_of_active_sims(data):
    """
    Fetches the total number of service_provider's that got triggered from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering service provider's (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering service provider's (format: YYYY-MM-DD).
    Returns:
        dict: containing the status of total_emails_count and the data card information.
    """
    service_provider = data.get("service_provider")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    if service_provider == "All":
        query = """
            SELECT COUNT(*)
            FROM sim_management_inventory
            WHERE sim_status IN ('Activated', 'Active')
            AND created_date BETWEEN %s AND %s
        """
        params = [
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    else:
        query = """
            SELECT COUNT(*)
            FROM sim_management_inventory
            WHERE service_provider = %s
            AND sim_status IN ('Activated', 'Active')
            AND created_date BETWEEN %s AND %s
        """
        params = [
            service_provider,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    # Database connection
    # database = DB('common_utils', **db_config)
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            active_sims_count = int(res.iloc[0, 0])  # Convert to standard Python int
        else:
            active_sims_count = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "No: of Active SIMs",
                "chart_type": "data",
                "data": active_sims_count,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except Exception as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching total emails",
        }
    return response



def count_of_pending_sim_activations(data):
    """
    Fetches the total number of sim activations's that got triggered from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering service provider's (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering service provider's (format: YYYY-MM-DD).
    Returns:
        dict: containing the status of total_emails_count and the data card information.
    """
    service_provider = data.get("service_provider")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    # common_utils_database = DB('common_utils', **db_config)
    if service_provider == "All":
        query = """
            SELECT COUNT(*)
            FROM sim_management_inventory
            WHERE sim_status='Pending Activation'
            AND created_date BETWEEN %s AND %s
        """
        params = [
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    else:
        query = """
            SELECT COUNT(*)
            FROM sim_management_inventory
            WHERE service_provider = %s
            AND sim_status='Pending Activation'
            AND created_date BETWEEN %s AND %s
        """
        params = [
            service_provider,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]
    # Database connection
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            pending_sim_activations_count = int(
                res.iloc[0, 0]
            )  # Convert to standard Python int
        else:
            pending_sim_activations_count = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "Pending SIM Activations",
                "chart_type": "data",
                "data": pending_sim_activations_count,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except Exception as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching total emails",
        }
    return response


def device_status_chart(data):
    """
    Fetches the count of device statuses over a custom date range.

    Args:
        data (dict): A dictionary containing:
            - partner_name (str): Partner name for filtering data.
            - start_date (str): Start date for filtering data (format: YYYY-MM-DD).
            - end_date (str): End date for filtering data (format: YYYY-MM-DD).
            - service_provider (str): Optional, filter by service provider.

    Returns:
        dict: A dictionary with the status of the data fetch and bar chart data.
    """

    # Parse the custom date range from the input
    start_date = datetime.strptime(data["start_date"], "%Y-%m-%d").date()
    end_date = datetime.strptime(data["end_date"], "%Y-%m-%d").date()
    service_provider = data.get("service_provider")

    # Build the query dynamically based on the service provider filter
    if service_provider.lower() == "all":
        query = """
        SELECT
         CASE
        WHEN Upper(sim_status) IN ('ACTIVATED', 'ACTIVE') THEN 'ACTIVE'
        ELSE Upper(sim_status) -- Keep the original status if no mapping is specified
        END AS sim_status,
        COUNT(*) AS status_count
        FROM sim_management_inventory
        WHERE
                    created_date BETWEEN %s AND %s
        GROUP BY
            CASE
            WHEN Upper(sim_status) IN ('ACTIVATED', 'ACTIVE') THEN 'ACTIVE'
            ELSE Upper(sim_status)
        END;

        """
        params = [start_date, end_date]
    else:
        query = """
        SELECT
         CASE
        WHEN Upper(sim_status) IN ('ACTIVATED', 'ACTIVE') THEN 'ACTIVE'
        ELSE Upper(sim_status) -- Keep the original status if no mapping is specified
        END AS sim_status,
        COUNT(*) AS status_count
        FROM sim_management_inventory
        WHERE
            service_provider_display_name = %s
            AND created_date BETWEEN %s AND %s
        GROUP BY
            CASE
            WHEN Upper(sim_status) IN ('ACTIVATED', 'ACTIVE') THEN 'ACTIVE'
            ELSE Upper(sim_status)
        END;
        """
        params = [service_provider, start_date, end_date]

    try:
        # Database connection
        tenant_database = data.get("db_name", "")
        database = DB(tenant_database, **db_config)

        # Execute the main query and retrieve the result
        res = database.execute_query(query, params=params)

        # Function to convert text to title case (with spaces)
        def to_title_case(text):
            return " ".join([word.capitalize() for word in text.split()])

        # Apply the function to the 'sim_status' column
        res["sim_status"] = res["sim_status"].apply(to_title_case)
        # Convert query result into a dictionary, handling any empty results
        status_counts = (
            {row["sim_status"]: row["status_count"] for row in res.to_dict("records")}
            if not res.empty
            else {}
        )

        # Extract all statuses dynamically from the query result
        statuses = list(status_counts.keys())

        # Prepare the response data
        data_card = []
        for status in statuses:  # Use dynamically fetched statuses
            count = status_counts.get(status, 0)  # Default to 0 if no data for status
            data_card.append({"status": status, "count": count})

        # Response formatted for bar chart data
        response = {
            "flag": True,
            "data": {
                "title": "Device Status",
                "chart_type": "bar",
                "data": data_card,
                "xField": "status",
                "yField": "count",
                "smooth": True,
                "height": 300,
                "width": 500,
            },
        }

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": True,
            "data": {
                "title": "Device Status",
                "chart_type": "bar",
                "data": [],
                "xField": "status",
                "yField": "count",
                "smooth": True,
                "height": 300,
                "width": 500,
            },
        }

    return response


def activated_vs_deactivated_pie_chart(data):
    """
    Fetches the count of 'Activated' vs 'Deactivated' devices over a custom date range for a pie chart.

    Args:
        data (dict): A dictionary containing:
            - partner_name (str): Partner name for filtering data.
            - start_date (str): Start date for filtering data (format: YYYY-MM-DD).
            - end_date (str): End date for filtering data (format: YYYY-MM-DD).
            - service_provider (str): Optional, filter by service provider.

    Returns:
        dict: A dictionary with the status of the data fetch and pie chart data.
    """

    # Parse the custom date range from the input
    start_date = datetime.strptime(data["start_date"], "%Y-%m-%d").date()
    end_date = datetime.strptime(data["end_date"], "%Y-%m-%d").date()
    service_provider = data.get("service_provider")

    # Adjust query to focus on 'Activated' and 'Deactivated' statuses
    if service_provider.lower() == "all":
        query = """

        SELECT
            sim_status,
            COUNT(*) AS status_count
        FROM (
            SELECT
                CASE
                    WHEN sim_status IN ('Activated', 'Active') THEN 'Activated'
                    ELSE sim_status
                END AS sim_status
            FROM sim_management_inventory
            WHERE sim_status IN ('Activated', 'Deactivated', 'Active')
            AND created_date BETWEEN %s AND %s
        ) AS grouped_status
        GROUP BY sim_status
        """
        params = [start_date, end_date]
    else:
        query = """
        SELECT
            sim_status,
            COUNT(*) AS status_count
        FROM (
            SELECT
                CASE
                    WHEN sim_status IN ('Activated', 'Active') THEN 'Activated'
                    ELSE sim_status
                END AS sim_status
            FROM sim_management_inventory
            WHERE sim_status IN ('Activated', 'Deactivated', 'Active')
            AND service_provider_display_name = %s AND created_date BETWEEN %s AND %s
        ) AS grouped_status
        GROUP BY sim_status
        """
        params = [service_provider, start_date, end_date]

    try:
        # Database connection
        tenant_database = data.get("db_name", "")
        database = DB(tenant_database, **db_config)

        # Execute the query and retrieve the result
        res = database.execute_query(query, params=params)

        # Convert query result into a dictionary, handling any empty results
        status_counts = (
            {row["sim_status"]: row["status_count"] for row in res.to_dict("records")}
            if not res.empty
            else {}
        )

        # Prepare data for pie chart
        pie_data = [
            {"status": "Activated", "count": status_counts.get("Activated", 0)},
            {"status": "Deactivated", "count": status_counts.get("Deactivated", 0)},
        ]

        # Response formatted for pie chart data
        response = {
            "flag": True,
            "data": {
                "title": "Activated vs Deactivated",
                "chart_type": "pie",
                "data": pie_data,
                "angleField": "count",
                "colorField": "status",
                "height": 300,
                "width": 300,
            },
        }

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": True,
            "data": {
                "title": "Activated vs Deactivated",
                "chart_type": "pie",
                "data": [],
                "angleField": "count",
                "colorField": "status",
                "height": 300,
                "width": 300,
            },
        }

    return response


def service_provider_change_request_stack_bar(data):
    """
    Fetches data for a stacked bar chart of change request types per service provider.

    Args:
        data (dict): A dictionary containing:
            - start_date (str): Start date for filtering data (format: YYYY-MM-DD).
            - end_date (str): End date for filtering data (format: YYYY-MM-DD).
            - db_name (str): The database name to connect to.

    Returns:
        dict: A dictionary with the status of the data fetch and stacked bar chart data.
    """

    # Parse date range from input
    start_date = datetime.strptime(data["start_date"], "%Y-%m-%d").date()
    end_date = datetime.strptime(data["end_date"], "%Y-%m-%d").date()
    tenant_database = data.get("db_name", "")
    service_provider = data.get("service_provider")

    # Adjust query to focus on 'Activated' and 'Deactivated' statuses
    if service_provider.lower() == "all":
        query = """
                SELECT
                    service_provider,
                    change_request_type,
                    COUNT(*) AS request_count
                FROM
                    sim_management_bulk_change
                WHERE
                    created_date BETWEEN %s AND %s
                GROUP BY
                    service_provider, change_request_type;
                """
        params = [start_date, end_date]
    else:
        query = """
                SELECT
                    service_provider,
                    change_request_type,
                    COUNT(*) AS request_count
                FROM
                    sim_management_bulk_change
                WHERE service_provider = %s
                    AND
                    created_date BETWEEN %s AND %s
                GROUP BY
                    service_provider, change_request_type;
                """
        params = [service_provider, start_date, end_date]
    try:
        # Database connection
        database = DB(tenant_database, **db_config)

        # Execute the query and retrieve the result
        res = database.execute_query(query, params=params)

        # Convert query result into structured data for stacked bar chart
        if not res.empty:
            chart_data = []
            for row in res.to_dict("records"):
                chart_data.append(
                    {
                        "service_provider": row["service_provider"],
                        "change_request_type": row["change_request_type"],
                        "count": row["request_count"],
                    }
                )

            # Response formatted for stacked bar chart
            response = {
                "flag": True,
                "data": {
                    "title": "Change Request Types Per Service Provider",
                    "chart_type": "stackedBar",
                    "data": chart_data,
                    "xField": "service_provider",
                    "yField": "count",
                    "seriesField": "change_request_type",
                    "height": 300,
                    "width": 500,
                },
            }
        else:
            # Empty result set
            response = {
                "flag": True,
                "data": {
                    "title": "Change Request Types Per Service Provider",
                    "chart_type": "stackedBar",
                    "data": [],
                    "xField": "service_provider",
                    "yField": "count",
                    "seriesField": "change_request_type",
                    "height": 300,
                    "width": 500,
                },
            }

    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": True,
            "data": {
                "title": "Change Request Types Per Service Provider",
                "chart_type": "stackedBar",
                "data": [],
                "xField": "service_provider",
                "yField": "count",
                "seriesField": "change_request_type",
                "height": 300,
                "width": 500,
            },
        }

    return response


def count_of_active_customers(data):
    """
    Fetches the total number of active customer's that got triggered from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering service provider's (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering service provider's (format: YYYY-MM-DD).
    Returns:
        dict: containing the status of total_emails_count and the data card information.
    """
    customer_name = data.get("customer_name")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)
    if customer_name == "All":
        query = """
            SELECT COUNT(*)
            FROM customers
            WHERE customer_name = %s
            AND is_active=true
            AND created_date BETWEEN %s AND %s
        """
        params = [
            customer_name,
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]

    else:
        query = """
            SELECT COUNT(*)
            FROM customers
            WHERE is_active=true
            AND created_date BETWEEN %s AND %s
        """
        params = [
            start_date,
            (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
                "%Y-%m-%d"
            ),
        ]

    # Database connection
    try:
        # Execute query with parameters
        res = database.execute_query(query, params=params)
        # Fetch the count and ensure it's a standard Python int
        if isinstance(res, pd.DataFrame) and not res.empty:
            active_customer_count = int(
                res.iloc[0, 0]
            )  # Convert to standard Python int
        else:
            active_customer_count = 0
        # Prepare the response
        response = {
            "flag": True,
            "data": {
                "title": "No: of Active Customers",
                "chart_type": "data",
                "data": active_customer_count,
                "icon": "useroutlined",
                "height": 100,
                "width": 300,
            },
        }
    except Exception as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": False,
            "message": "Something went wrong fetching total emails",
        }
    return response


def rev_assurance_record_discrepancy_card(data):
    """
    Fetches the total number of service_provider's that got triggered from the AMOP application.
    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering service provider's (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering service provider's (format: YYYY-MM-DD).
    Returns:
        dict: containing the status of total_emails_count and the data card information.
    """
    service_provider = data.get("service_provider")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    tenant_database = data.get("db_name", "")
    database = DB(tenant_database, **db_config)

    # Check if the end_date is provided and valid
    if end_date is None:
        response = {
            "flag": False,
            "message": "End date is required and cannot be None.",
        }
        return response

    # If end_date is not None, try to parse it
    try:
        # Handle cases where end_date includes time (e.g., 'YYYY-MM-DD HH:MM:SS')
        end_date_stripped = end_date.split()[0]  # Keep only the date part
        end_date_parsed = datetime.strptime(end_date_stripped, "%Y-%m-%d")
        end_date_parsed = end_date_parsed + timedelta(days=1)
    except ValueError as e:
        response = {"flag": False, "message": f"Invalid end date format: {str(e)}"}
        return response

    # Base query depending on whether 'service_provider' is 'All' or specific
    if service_provider == "All":
        query = """
            SELECT
                service_provider,
                CASE
                    WHEN LOWER(device_status) = '' THEN CONCAT('Device not in Rev.IO, but ', 'activated', ' in carrier.')
                    WHEN LOWER(rev_io_status) = 'deactivated' THEN CONCAT('Device is Deactivated in Rev.IO, but ', device_status, ' in carrier.')
                    WHEN LOWER(device_status) = 'deactivated' THEN CONCAT('Device is Deactivated in carrier, but ', 'activated', ' in Rev.IO.')
                    ELSE CONCAT('Device is ', device_status, ' in both Rev.IO and carrier.')
                END AS message,
                COUNT(*) AS message_count
            FROM
                vw_rev_assurance_list_view_with_count
            WHERE
              carrier_last_status_date BETWEEN %s AND %s
            GROUP BY
                service_provider, message
            ORDER BY
                service_provider, message_count DESC;
        """
        params = [start_date, end_date_parsed.strftime("%Y-%m-%d")]
    else:
        query = """
            SELECT
                service_provider,
                CASE
                    WHEN LOWER(device_status) = '' THEN CONCAT('Device not in Rev.IO, but ', 'activated', ' in carrier.')
                    WHEN LOWER(rev_io_status) = 'deactivated' THEN CONCAT('Device is Deactivated in Rev.IO, but ', device_status, ' in carrier.')
                    WHEN LOWER(device_status) = 'deactivated' THEN CONCAT('Device is Deactivated in carrier, but ', 'activated', ' in Rev.IO.')
                    ELSE CONCAT('Device is ', device_status, ' in both Rev.IO and carrier.')
                END AS message,
                COUNT(*) AS message_count
            FROM
                vw_rev_assurance_list_view_with_count
            WHERE
                service_provider = %s AND carrier_last_status_date BETWEEN %s AND %s
            GROUP BY
                service_provider, message
            ORDER BY
                service_provider, message_count DESC;
        """
        params = [service_provider, start_date, end_date_parsed.strftime("%Y-%m-%d")]

    try:
        # Execute the query
        res = database.execute_query(query, params=params)

        # If the result is not empty
        if isinstance(res, pd.DataFrame) and not res.empty:
            # Initialize a dictionary to hold the data
            grouped_data = {}

            # Group the data by service provider
            for row in res.to_dict("records"):
                provider = row["service_provider"]
                message = row["message"]
                message_count = row["message_count"]

                # If the provider is not in the dictionary, initialize it
                if provider not in grouped_data:
                    grouped_data[provider] = {"provider": provider, "messages": {}}

                # Add the message count under the provider
                grouped_data[provider]["messages"][message] = message_count

            # Prepare the final response
            response = {
                "title": "Rev Assurance-Record Discrepancies",
                "chart_type": "bar_two",
                "flag": True,
                "data": list(grouped_data.values()),
                "xField": "Service Provider",
                "yField": "No.of records",
                "height": 400,
                "width": 800,
            }
        else:
            # Handle case where no data is found
            response = {
                "flag": True,  # Flag is True to indicate the request was successful
                "title": "Rev Assurance-Record Discrepancies",
                "chart_type": "bar_two",
                "data": [],  # Empty data array
                "xField": "Service Provider",  # Column name for x-axis
                "yField": "No.of records",  # Column name for y-axis
                "height": 400,
                "width": 800,
            }
    except Exception as e:
        logging.exception("Exception occurred: %s", e)
        response = {
            "flag": True,  # Flag is True to indicate the request was successful
            "title": "Rev Assurance-Record Discrepancies",
            "chart_type": "bar_two",
            "data": [],  # Empty data array
            "xField": "Service Provider",  # Column name for x-axis
            "yField": "No.of records",  # Column name for y-axis
            "height": 400,
            "width": 800,
        }

    return response

def daily_sync_card(data):
    """
    Fetches the sync status details for migrations from the database and formats the result.

    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering records (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering records (format: YYYY-MM-DD).

    Returns:
        dict: Contains the formatted data for display in the desired structure.
    """
    columns = [
        {"title": "S. No", "dataIndex": "no", "key": "no", "width": "10%"},
        {
            "title": "Sync Name",
            "dataIndex": "syncName",
            "key": "syncName",
            "width": "25%",
        },
        {
            "title": "Sync Type",
            "dataIndex": "syncType",
            "key": "syncType",
            "width": "20%",
        },
        {"title": "Status", "dataIndex": "status", "key": "status", "width": "20%"},
        {
            "title": "Date and Time",
            "dataIndex": "dateTime",
            "key": "dateTime",
            "width": "25%",
        },
    ]
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    mg_database = DB("Migration_Test", **db_config)

    # Log the received input data for debugging
    logging.info(f"Received data: {data}")

    # Ensure the end date includes the whole day by adding 1 day
    end_date_adjusted = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # Log the adjusted end date
    logging.info(f"Adjusted end date: {end_date_adjusted}")

    # Base query with conditions, using placeholders for date range
    query = """
        SELECT
            migration_name AS "sync_status",
            CASE
                WHEN schedule_flag = TRUE AND reverse_sync = TRUE THEN 'Reverse Sync'
                WHEN schedule_flag = TRUE THEN 'Daily Sync'
                ELSE 'No Sync'
            END AS "sync_type",
            CASE
                WHEN status = 'true' THEN 'Success'
                WHEN status = 'false' THEN 'Failure'
                ELSE 'No Sync upto now'
            END AS "status",
            created_date AS "date_and_time"
        FROM public.migrations_altaworx_test
        WHERE to_database = 'altaworx_test'
    """

    # Log the query for debugging purposes
    logging.info(f"Executing query: {query}")
    logging.info(f"Using params: {start_date}, {end_date_adjusted}")

    params = [start_date, end_date_adjusted]

    try:
        # Execute the query with the parameters
        res = mg_database.execute_query(query, params=params)

        # Log the columns of the DataFrame to check their names
        logging.info(f"Columns in the DataFrame: {res.columns}")

        # Strip any leading/trailing spaces in column names
        res.columns = res.columns.str.strip()

        # Log columns again after stripping spaces
        logging.info(f"Columns after stripping spaces: {res.columns}")

        # If the result is not empty
        if isinstance(res, pd.DataFrame) and not res.empty:
            # Initialize a list to hold the formatted data
            formatted_data = []

            # Define columns dynamically
            columns = [
                {"title": "S. No", "dataIndex": "no", "key": "no", "width": "10%"},
                {
                    "title": "Sync Name",
                    "dataIndex": "syncName",
                    "key": "syncName",
                    "width": "25%",
                },
                {
                    "title": "Sync Type",
                    "dataIndex": "syncType",
                    "key": "syncType",
                    "width": "20%",
                },
                {
                    "title": "Status",
                    "dataIndex": "status",
                    "key": "status",
                    "width": "20%",
                },
                {
                    "title": "Date and Time",
                    "dataIndex": "dateTime",
                    "key": "dateTime",
                    "width": "25%",
                },
            ]

            # Iterate over the rows of the result and format dynamically
            for idx, row in res.iterrows():
                formatted_row = {
                    "key": str(idx + 1),  # Use index as key for each row
                    "no": idx + 1,
                    "syncName": row["sync_status"],  # Match the column name exactly
                    "syncType": row["sync_type"],  # Match the column name exactly
                    "status": row["status"],  # Match the column name exactly
                    "dateTime": row["date_and_time"].strftime(
                        "%d/%m/%y %I:%M:%S %p"
                    ),  # Use strftime() directly
                }
                formatted_data.append(formatted_row)

            # Prepare the final response
            response = {
                "flag": True,
                "data": formatted_data,
                "columns": columns,
                "height": 400,
                "width": 800,
            }
        else:
            # Handle case where no data is found
            logging.warning(f"No data found for the given criteria: {params}")
            response = {
                "flag": True,  # Flag is True to indicate the request was successful
                "data": [],  # Empty data array
                "columns": columns,  # Keep the column structure the same
                "height": 400,
                "width": 800,
            }
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": True,  # Flag is True to indicate the request was successful
            "data": [],  # Empty data array
            "columns": columns,  # Keep the column structure the same
            "height": 400,
            "width": 800,
        }

    # Log the response for debugging
    # logging.info(f"Response: {response}")

    return response


def live_sessions_table(data):
    """
    Fetches the live session details from the database and formats the result.

    Args:
        data (dict): A dictionary containing the following keys:
            - service_provider (str): The service_provider for filtering service provider's.
            - start_date (str): The start date for filtering records (format: YYYY-MM-DD).
            - end_date (str): The end date for filtering records (format: YYYY-MM-DD).

    Returns:
        dict: Contains the formatted data for display in the desired structure.
    """
    columns = [
        {"title": "S. No", "dataIndex": "no", "key": "no", "width": "10%"},
        {
            "title": "Username",
            "dataIndex": "username",
            "key": "username",
            "width": "25%",
        },
        {"title": "Login", "dataIndex": "login", "key": "login", "width": "25%"},
        {"title": "Logout", "dataIndex": "logout", "key": "logout", "width": "20%"},
        {
            "title": "Last Request",
            "dataIndex": "lastRequest",
            "key": "lastRequest",
            "width": "20%",
        },
    ]
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    common_utils_database = DB("common_utils", **db_config)

    # Log the received input data for debugging
    logging.info(f"Received data: {data}")

    # Ensure the end date includes the whole day by adding 1 day
    end_date_adjusted = (
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # Log the adjusted end date
    logging.info(f"Adjusted end date: {end_date_adjusted}")

    # Base query with conditions, using placeholders for date range
    query = """
                   WITH LatestSessions AS (
            SELECT username,
                   TO_CHAR(login::date, 'YYYY-MM-DD HH:SS') AS login,
                   TO_CHAR(logout::date, 'YYYY-MM-DD HH:SS') AS logout,
                   TO_CHAR(last_request::date, 'YYYY-MM-DD HH:SS') AS last_request,
                   ROW_NUMBER() OVER (PARTITION BY username ORDER BY last_request DESC) AS rn
            FROM public.live_sessions
            WHERE last_request BETWEEN %s AND %s
        )
        SELECT username, login, logout, last_request
        FROM LatestSessions
        WHERE rn = 1
        ORDER BY last_request DESC;
    """

    # Log the query for debugging purposes
    logging.info(f"Executing query: {query}")
    logging.info(f"Using params: {start_date}, {end_date_adjusted}")

    try:
        # Execute the query with the parameters
        res = common_utils_database.execute_query(
            query, params=[start_date, end_date_adjusted]
        )

        # Log the columns of the DataFrame to check their names
        logging.info(f"Columns in the DataFrame: {res.columns}")

        # Strip any leading/trailing spaces in column names
        res.columns = res.columns.str.strip()

        # Log columns again after stripping spaces
        logging.info(f"Columns after stripping spaces: {res.columns}")

        # If the result is not empty
        if isinstance(res, pd.DataFrame) and not res.empty:
            # Initialize a list to hold the formatted data
            formatted_data = []

            # Define columns dynamically
            columns = [
                {"title": "S. No", "dataIndex": "no", "key": "no", "width": "10%"},
                {
                    "title": "Username",
                    "dataIndex": "username",
                    "key": "username",
                    "width": "25%",
                },
                {
                    "title": "Login",
                    "dataIndex": "login",
                    "key": "login",
                    "width": "25%",
                },
                {
                    "title": "Logout",
                    "dataIndex": "logout",
                    "key": "logout",
                    "width": "20%",
                },
                {
                    "title": "Last Request",
                    "dataIndex": "lastRequest",
                    "key": "lastRequest",
                    "width": "20%",
                },
            ]

            # Iterate over the rows of the result and format dynamically
            for idx, row in res.iterrows():
                formatted_row = {
                    "key": str(idx + 1),  # Use index as key for each row
                    "no": idx + 1,
                    "username": row[
                        "username"
                    ],  # Column names should match the query result
                    "login": row["login"],
                    "logout": row["logout"],
                    "lastRequest": (
                        str(row["last_request"])
                        if isinstance(row["last_request"], pd.Timestamp)
                        else row["last_request"]
                    ),
                }
                formatted_data.append(formatted_row)

            # Prepare the final response
            response = {
                "flag": True,
                "data": formatted_data,
                "columns": columns,
                "height": 400,
                "width": 800,
            }
        else:
            # Handle case where no data is found
            logging.warning(
                f"No data found for the given criteria: {start_date}, {end_date_adjusted}"
            )
            response = {
                "flag": True,  # Flag is True, since the structure is valid
                "data": [],  # Empty data array
                "columns": columns,  # Keep the column structure the same
                "height": 400,
                "width": 800,
            }
    except Exception as e:
        logging.exception(f"Exception occurred: {e}")
        response = {
            "flag": True,  # Flag is True, since the structure is valid
            "data": [],  # Empty data array
            "columns": columns,  # Keep the column structure the same
            "height": 400,
            "width": 800,
        }

    # Log the response for debugging
    logging.info(f"Response: {response}")

    return response
