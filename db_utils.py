"""
@Author : Nikhil .N
@Author : Phaneendra.Y
Created Date: 21-06-24
"""

# Importing the necessary Libraries
import psycopg2
from time import time
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, exc
from sqlalchemy.exc import OperationalError
from time import time
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    select,
    desc,
    asc,
    and_,
    update,
    or_,
    insert,
    func,
)
from sqlalchemy import (
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
    Column,
    Table,
    MetaData,
)
from sqlalchemy import insert
from common_utils.logging_utils import Logging
import numpy as np
import json
import os

logging = Logging(name="db_utils")


##test jenkins
class DB(object):
    def __init__(
        self,
        database,
        host="127.0.0.1",
        user="root",
        password="",
        port="3306",
        db_type="postgresql",
    ):
        """
        Initialization of databse object.

        Args:
            databse (str): The database to connect to.
            host (str): Host IP address. For dockerized app, it is the name of
                the service set in the compose file.
            user (str): Username of PostgresSQL server. (default = 'root')
            password (str): Password of PostgresSQL server. For dockerized app, the
                password is set in the compose file. (default = '')
            port (str): Port number for PostgresSQL. For dockerized app, the port that
                is mapped in the compose file. (default = '3306')
        """
        # Assigning instance variables with the given arguments or default values
        self.HOST = host
        self.USER = user
        self.PASSWORD = password
        self.PORT = port
        self.DATABASE = f"{database}"
        self.DB_TYPE = db_type
        # Logging the connection details for debugging purposes
        # logging.info(f'Host: {self.HOST}')
        # logging.info(f'User: {self.USER}')
        # logging.info(f'Port: {self.PORT}')
        # logging.info(f'Database: {self.DATABASE}')
        # Calling the connect method to establish a connection to the database
        self.connect()
        file_path = f"/opt/python/lib/python3.9/site-packages/common_utils/schemas/{database}.json"

        # Check if the file exists
        if not os.path.exists(file_path):
            print(
                f"File for '{database}' not found. Using default 'default_schema' instead."
            )
            # Replace with default database if file is not found
            file_path = f"/opt/python/lib/python3.9/site-packages/common_utils/schemas/default_schema.json"
        self.metadata, self.tables = self.load_schema(file_path)

    def __del__(self):
        # This method is called when the object is about to be destroyed
        try:
            logging.info("############### DETROYING THE DB CONNECTION")
            # Attempting to close the database connection
            self.engine.close()
            # Disposing of the database engine to free up resources
            # if self.engine:
            #     self.engine.dispose()
            logging.info("connections are closed")

        except Exception as e:
            logging.exception("########### Failed to destroy the DB COnenctions", e)

    def connect(self, max_retry=3):
        retry = 1
        try:
            start = time()
            logging.info(f"Making connection to `{self.DATABASE}`...")
            config = f"postgresql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"
            # logging.info(f"connection : {config}")
            self.engine = create_engine(
                config, connect_args={"connect_timeout": 10}, pool_recycle=300
            )
            logging.info(f"Engine created for `{self.DATABASE}`")
            while retry <= max_retry:
                try:
                    self.engine = self.engine.connect()
                    self.session = sessionmaker(bind=self.engine)()
                    logging.info(
                        f"Connection established successfully to `{self.DATABASE}`! ({round(time() - start, 2)} secs to connect)"
                    )
                    break
                except Exception as e:
                    logging.warning(f"Connection failed. Retrying... ({retry}) [{e}]")
                    retry += 1
                    self.engine.dispose()
                    if retry > max_retry:
                        raise Exception(
                            "Max retry attempts reached. Could not connect to the database."
                        )
        except Exception as e:
            raise Exception(f"Something went wrong while connecting. Check trace. {e}")

    def type_from_string(self, type_str):
        # Map type strings to SQLAlchemy types
        type_map = {
            "Integer": Integer,
            "String": String,
            "Float": Float,
            "DateTime": DateTime,
            "Boolean": Boolean,
            # Add other types as needed
        }
        return type_map.get(type_str, String)  # Default to String if type is not found

    def load_schema(self, schema_file):
        metadata = MetaData()
        tables = {}

        with open(schema_file, "r") as f:
            schema = json.load(f)

        for table_name, columns in schema.items():
            column_objects = []
            for col in columns:
                col_type = self.type_from_string(
                    col["type"]
                )  # Convert type string to actual type
                column_objects.append(
                    Column(
                        col["name"],
                        col_type,
                        nullable=col["nullable"],
                        primary_key=col["primary_key"],
                    )
                )

            tables[table_name] = Table(table_name, metadata, *column_objects)

        return metadata, tables

    def combine_records_excluding_columns(
        self, data, exclude_columns=["id", "schema_tag"]
    ):
        # Create a DataFrame from the input data
        df = pd.DataFrame(data)

        # Determine which columns to include by excluding the specified columns
        include_columns = df.columns.difference(exclude_columns)

        # Group by the include_columns and perform aggregation
        result = df.groupby(include_columns.tolist(), as_index=False).agg(
            id=("id", lambda x: "-".join(map(str, x))),  # Join original IDs
            schema_tag=(
                "schema_tag",
                lambda x: "-".join(set(x)),
            ),  # Aggregate schema tags
        )

        return result

    def fetch_schemas(self):
        schema_query = """
        SELECT nspname AS schema_name
        FROM pg_catalog.pg_namespace
        WHERE nspname NOT LIKE 'pg_%'
        AND nspname != 'information_schema';
        """

        with self.engine as connection:
            schemas_result = connection.execute(text(schema_query)).fetchall()

        schemas = [
            row[0] for row in schemas_result
        ]  # Getting the result returns a list of schema names
        if not schemas:
            print("No schemas found.")
            return []
        return schemas

    def set_shema_path(self, schema):
        # Set the search path to the schema
        set_path = f"SET search_path TO {schema}"
        try:
            if self.engine.closed:
                print("Connection is closed. Attempting to reconnect...")
                self.connect()
            pd.read_sql(set_path, self.engine)
        except exc.ResourceClosedError:
            print("Query executed, but it does not have any result to return.")
            return None

    def execute_query_multi_schemas(
        self, query, flag=False, insert=[], update=[], schemas=[], attempt=0, **kwargs
    ):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            flag (bool): A flag to indicate if the query should proceed without parameters.
            direct_query (bool): If True, executes the query across all available schemas.
            attempt (int): Number of attempts made for retries in case of connection errors.
            kwargs: Additional arguments like 'params' for the query.

        Returns:
            (DataFrame): A pandas DataFrame containing the result of the query, or None if an error occurs.
        """

        try:
            # Initialize data variable
            data = None
            print(
                f"Executing Query in execute_query_multi_schemas: {query} and insert is {insert} and update is {update} and schemas is {schemas}"
            )

            combined_results = []
            # Handle direct query for executing across schemas
            if insert:
                schemas = insert
            elif update:
                schemas = update

            for schema in schemas:
                try:
                    # Check if the query is parameterized
                    if "params" not in kwargs and not flag:
                        print("Cannot execute query (Expecting parameterized query).")
                        return None

                    # Extract parameters from kwargs, if available
                    params = tuple(kwargs.get("params", ()))
                    print(f"Params: {params}")

                    print(f"Running query on schema: {schema}")
                    self.set_shema_path(schema)

                    if hasattr(query, "compile"):
                        query_str = str(
                            query.compile(compile_kwargs={"literal_binds": True})
                        )
                    else:
                        query_str = str(query)

                    print(f"####### Query: {query_str}")
                    print(f"####### Params: {params}")

                    # Execute the query using pandas read_sql method
                    try:
                        query_str = query_str.replace("`", "")
                        query_str = query_str.replace('"', "")
                    except:
                        pass

                    if self.engine.closed:
                        print("Connection is closed. Attempting to reconnect...")
                        self.connect()

                    # Execute the query using pandas read_sql method
                    schema_data = pd.read_sql(query_str, self.engine, params=params)
                    print(f"Query executed successfully.and data is {schema_data}")

                    if not insert and not update:
                        schema_data["schema_tag"] = schema
                        combined_results.append(schema_data)
                except exc.ResourceClosedError:
                    print("Query executed, but it does not have any result to return.")

            # Combine results across schemas and return as one DataFrame
            if combined_results:
                return self.combine_records_excluding_columns(
                    pd.concat(combined_results, ignore_index=True)
                )
            else:
                print("No results obtained across schemas.")
                return True

        except exc.ResourceClosedError:
            print("Query executed, but it does not have any result to return.")
            return None

        except exc.IntegrityError as e:
            print(f"Integrity Error: {e}")
            return None

        except (exc.StatementError, OperationalError) as e:
            # Handle statement and operational errors (e.g., connection issues)
            print(f"Error encountered: {e}. Retrying (attempt #{attempt + 1})...")
            attempt += 1
            if attempt <= 3:
                # Retry the connection and execution
                self.connect()
                return self.execute_query(
                    query,
                    flag=flag,
                    insert=insert,
                    update=update,
                    attempt=attempt,
                    **kwargs,
                )
            else:
                print("Maximum retry attempts reached.")
                return None

        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def execute_query(
        self, query, flag=False, insert=[], update=[], attempt=0, **kwargs
    ):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        # Initialize data variable
        data = None
        print(f"Query: {query}")
        schemas = []
        if not insert and not update:
            schemas = self.fetch_schemas()
        print(f"schemas: {schemas}")

        if len(schemas) > 1 or len(update) > 0 or len(insert) > 1:
            return self.execute_query_multi_schemas(
                query, flag, insert, update, schemas
            )
        Session = sessionmaker(bind=self.engine)
        session = Session()  # Create a new session
        try:
            # Check if the query is parameterized
            if "params" not in kwargs and not flag:
                print("Cannot execute query (Expecting parametarized query).")
                return True
            # Extract parameters from kwargs if available
            params = kwargs["params"] if "params" in kwargs else None
            if params:
                params = tuple(params)
            else:
                params = ()

            if self.engine.closed:
                print("Connection is closed. Attempting to reconnect...")
                self.connect()

            if hasattr(query, "compile"):
                query_str = str(query.compile(compile_kwargs={"literal_binds": True}))
            else:
                query_str = str(query)

            print(f"####### Query: {query_str}")
            print(f"####### Params: {params}")

            # Execute the query using pandas read_sql method
            try:
                query_str = query_str.replace("`", "")
                query_str = query_str.replace('"', "")
            except:
                pass
            data = pd.read_sql(query_str, self.engine, params=params)
            # data =  self.engine.execute(query, params=params)
            print(f"########### Data")
        except exc.ResourceClosedError:
            print("Query does not have any value to return.")
            return True
        except exc.IntegrityError as e:
            print(f"Integrity Error - {e}")
            return None
        except (exc.StatementError, OperationalError) as e:
            # Handle statement errors and operational errors (e.g., connection issues)
            print(f"Creating new connection. Engine/Connection is probably None. [{e}]")
            attempt += 1
            if attempt <= 3:
                # Reconnect to the database
                self.connect()
                print(f"Attempt #{attempt}")
                # Execute the query using execute_query
                return self.execute_query(query, attempt=attempt, **kwargs)
            else:
                print(f"Maximum attempts reached. ({3})")
                return False
        except Exception as e:
            print(f"Something went wrong executing query. Check trace. {e}")
            params = kwargs["params"] if "params" in kwargs else None
            return False
        finally:
            session.close()
            print("connections are closed")
        # Replace NaN values in the resulting DataFrame with None and return
        return data.replace({np.nan: None})

    def _build_filters(self, table, filters, combine_logic=[]):
        """
        Builds the filter conditions for a SQL query based on the provided filters
        and combines them using the specified logical operators (AND, OR).

        Args:
            table (Table): The SQLAlchemy table object that represents the table.
            filters (dict): A dictionary where keys are column names and values are
                            the filter conditions (e.g., 'value' or 'not Null').
            combine_logic (list): A list of indexes where the logic 'OR' should be applied
                                instead of the default 'AND'.

        Returns:
            Combined condition: The combined SQLAlchemy filter condition, which can
                                be used in a query's `where()` clause.
        """

        filter_conditions = (
            []
        )  # List to hold filter conditions and their associated logic
        count = 0  # Initialize counter for combine logic

        for key, value in filters.items():
            # Determine the logical operator based on combine_logic list
            if str(count) in combine_logic:
                logic = "OR"  # Use "OR" if the index is specified in combine_logic
            else:
                logic = "AND"  # Default logic is "AND"

            # Handle NULL and NOT NULL conditions
            if value == "not Null":
                filter_condition = table.c[key].isnot(
                    None
                )  # SQL condition: column IS NOT NULL
            elif value == "Null":
                filter_condition = table.c[key].is_(
                    None
                )  # SQL condition: column IS NULL
            elif isinstance(value, (list, tuple)):
                # Handle WHERE IN condition if the value is a list or tuple
                filter_condition = table.c[key].in_(value)
            else:
                # Handle equality condition
                filter_condition = table.c[key] == value

            count += 1  # Increment count to keep track of the next filter logic
            filter_conditions.append(
                (filter_condition, logic)
            )  # Store the condition and its logic

        # Separate filter conditions based on AND and OR logic
        and_conditions = [cond for cond, logic in filter_conditions if logic == "AND"]
        or_conditions = [cond for cond, logic in filter_conditions if logic == "OR"]

        # Combine conditions using AND and OR logic
        combined_condition = "None"
        if and_conditions:
            combined_condition = and_(*and_conditions)  # Combine all AND conditions

        if or_conditions:
            or_combined = or_(*or_conditions)  # Combine all OR conditions

            if combined_condition != "None":
                combined_condition = and_(
                    combined_condition, or_combined
                )  # Combine AND and OR conditions
            else:
                combined_condition = (
                    or_combined  # If no AND conditions, use only OR conditions
                )

        return combined_condition  # Return the final combined condition

    def get_data(
        self,
        table_name=None,
        condition=None,
        columns=None,
        order=None,
        combine=[],
        joins=None,
        mod_pages=None,
        concat_columns=None,
        coalesce_columns=None,
        distinct=False,
    ):
        """
        Retrieves data from a table in the database based on the provided parameters,
        such as conditions, columns to select, ordering, joins, pagination, and more.

        Args:
            table_name (str): The name of the table from which data is to be fetched.
            condition (dict): A dictionary of conditions to filter the data.
            columns (list): List of column names or expressions to retrieve.
            order (dict): Dictionary specifying the column and direction for ordering.
            combine (list): List of filter conditions that should be combined with "OR" logic.
            joins (dict): Dictionary of tables to join and the on-condition for the join.
            mod_pages (dict): Dictionary specifying pagination details ('start' and 'end').
            concat_columns (tuple): A tuple of columns to concatenate and generate a new field.
            coalesce_columns (list): A list of columns to apply the SQL COALESCE function on.
            distinct (bool): Whether to retrieve distinct rows.

        Returns:
            list or bool: A list of rows that satisfy the conditions, or False if an error occurs.
        """
        Session = sessionmaker(
            bind=self.engine
        )  # Create a session to interact with the database
        session = Session()  # Initialize the session

        try:
            print(
                table_name, condition, columns, order, joins
            )  # Print the input parameters for debugging

            # Retrieve the table object from the predefined tables dictionary
            table = self.tables.get(table_name)

            # If columns are provided, make sure each column is a valid SQLAlchemy expression
            if columns:
                columns = [
                    table.c[col] if isinstance(col, str) else col for col in columns
                ]
            else:
                columns = [
                    table
                ]  # If no columns are specified, select all columns from the table

            # Handle concatenation of columns if `concat_columns` is provided
            if concat_columns:
                name_column, id_column = concat_columns
                concat_expr = func.concat(
                    table.c[name_column], str(" - "), table.c[id_column]
                )
                columns.append(
                    concat_expr.label("concat_column")
                )  # Add the concatenated column to the selection

            # Apply the COALESCE function if `coalesce_columns` is provided
            if coalesce_columns:
                coalesce_expr = func.coalesce(
                    *[table.c[col] for col in coalesce_columns]
                )
                columns.append(
                    coalesce_expr.label("coalesce_column")
                )  # Add the COALESCE result to the selection

            # Create the base SQL query with the selected columns
            query = select(*columns)

            if distinct:
                query = query.distinct()  # Ensure that only distinct rows are returned

            # Handle table joins if `joins` is provided
            if joins:
                for join_table, on_condition in joins.items():
                    join_table = Table(
                        join_table, metadata, autoload_with=self.engine
                    )  # Reflect the join table
                    query = query.join(
                        join_table, on_condition
                    )  # Add the join to the query

            # Apply filters based on the `condition` parameter
            if condition:
                where = self._build_filters(table, condition, combine)
                query = query.where(where)

            # Apply ordering if `order` is specified
            if order:
                order_by = list(order.keys())[0]  # Extract column to order by
                order_direction = list(order.values())[
                    0
                ]  # Extract the order direction ('asc' or 'desc')
                order_clause = (
                    asc(order_by) if order_direction == "asc" else desc(order_by)
                )
                query = query.order_by(
                    order_clause
                )  # Add the ordering clause to the query

            # Apply pagination if `mod_pages` is provided
            if mod_pages:
                start = mod_pages.get("start", 0)  # Get the start index for pagination
                end = mod_pages.get("end", 500)  # Get the end index for pagination
                query = query.offset(start).limit(
                    end - start
                )  # Apply the pagination limits

            print(
                f"Final query that is going to get executed is {query}"
            )  # Print the final query for debugging
            return self.execute_query(
                query, True
            )  # Execute the query and return the result

        except Exception as e:
            print(f"Error getting data: {e}")  # Log any errors that occur
            return False  # Return False if an error occurs during the query execution
        finally:
            session.close()  # Close the session to free up resources

    def update_dict(
        self,
        table_name,
        values,
        and_conditions=None,
        or_conditions=None,
        in_conditions=None,
    ):
        """
        Updates a table in the database using SQLAlchemy.

        Parameters:
        - table_name: The name of the table to update.
        - values: A dictionary of column-value pairs to update.
        - and_conditions: A list of `AND` conditions (tuples of column and value).
        - or_conditions: A list of `OR` conditions (tuples of column and value).
        - in_conditions: A list of `IN` conditions (tuples of column and list of values).

        Example:
        update_table('users', {'name': 'New Name'},
                    and_conditions={'age':30},
                    or_conditions={'city':'New York'},
                    in_conditions={'id':[1, 2, 3]})
        """

        try:
            # Create connection string
            # metadata = MetaData()
            # metadata.bind = self.engine  # Bind the metadata to the engine
            # table = Table(table_name, metadata, autoload_with=self.engine)
            table = self.tables.get(table_name)

            # Create the base update statement
            stmt = update(table).values(**values)

            # Apply AND conditions
            if and_conditions:
                and_clause = and_(
                    *[
                        table.c[column] == value
                        for column, value in and_conditions.items()
                    ]
                )
                stmt = stmt.where(and_clause)

            # Apply OR conditions
            if or_conditions:
                or_clause = or_(
                    *[
                        table.c[column] == value
                        for column, value in or_conditions.items()
                    ]
                )
                stmt = stmt.where(or_clause)

            # Apply IN conditions
            if in_conditions:
                in_clause = and_(
                    *[
                        table.c[column].in_(values)
                        for column, values in in_conditions.items()
                    ]
                )
                stmt = stmt.where(in_clause)
            print(stmt)
            result = self.engine.execute(stmt)
            self.engine.commit()

            print("update successful")
            return True

        except Exception as e:
            # Logging an exception if any error occurs during the insertion process
            print(f"Error updating data: {e}")
            return False
        finally:
            # Ensure that the cursor and connection are closed
            if "conn" in locals() and self.engine:
                self.engine.close()

    def update_dict_back(
        self,
        table_name,
        values,
        and_conditions=None,
        or_conditions=None,
        in_conditions=None,
    ):
        """
        Updates a table in the database using SQLAlchemy.

        Parameters:
        - table_name: The name of the table to update.
        - values: A dictionary of column-value pairs to update.
        - and_conditions: A dictionary of `AND` conditions (tuples of column and value).
        - or_conditions: A dictionary of `OR` conditions (tuples of column and value).
        - in_conditions: A dictionary of `IN` conditions (tuples of column and list of values).

        Example:
        update_dict('users', {'name': 'New Name'},
                    and_conditions={'age': 30, 'id': '123-456-789'},
                    or_conditions={'city': 'New York'},
                    in_conditions={'id': ['123-456-789', '987-654-321']})
        """

        try:
            # Extract and split the schema_tag from values or conditions
            schema_tag = (
                values.pop("schema_tag", None)
                or (and_conditions and and_conditions.pop("schema_tag", None))
                or (or_conditions and or_conditions.pop("schema_tag", None))
                or (in_conditions and in_conditions.pop("schema_tag", None))
            )

            # Split schema_tag by '-' for index use
            schema_tag = schema_tag.split("-") if schema_tag else []
            for ind, tag in enumerate(schema_tag):
                print(f" schema_tag is {tag} and ind is {ind}")
                # Retrieve the table from the schema specified by schema_tag
                table = self.tables.get(table_name)

                # Function to split the id based on schema_tag index
                def process_id_condition(condition_dict):
                    condition = {}
                    for key, values_list in condition_dict.items():
                        condition[key] = values_list
                        if key == "id" and schema_tag:
                            condition["id"] = condition_dict["id"].split("-")[
                                ind
                            ]  # Use schema_tag index
                    return condition

                # Process the `and_conditions`, `or_conditions`, and `in_conditions`
                and_condition_dict = {}
                if and_conditions:
                    and_condition_dict = process_id_condition(and_conditions)
                or_condition_dict = {}
                if or_conditions:
                    or_condition_dict = process_id_condition(or_conditions)
                in_condition_dict = {}
                if in_conditions:
                    for key, values_list in in_conditions.items():
                        in_condition_dict[key] = values_list
                        if key == "id" and schema_tag:
                            in_condition_dict[key] = [
                                value.split("-")[ind] for value in values_list
                            ]

                print(and_condition_dict, or_condition_dict, in_condition_dict)

                # Create the base update statement
                stmt = update(table).values(**values)

                # Apply AND conditions
                if and_condition_dict:
                    and_clause = and_(
                        *[
                            table.c[column] == value
                            for column, value in and_condition_dict.items()
                        ]
                    )
                    stmt = stmt.where(and_clause)

                # Apply OR conditions
                if or_condition_dict:
                    or_clause = or_(
                        *[
                            table.c[column] == value
                            for column, value in or_condition_dict.items()
                        ]
                    )
                    stmt = stmt.where(or_clause)

                # Apply IN conditions
                if in_condition_dict:
                    in_clause = and_(
                        *[
                            table.c[column].in_(values)
                            for column, values in in_condition_dict.items()
                        ]
                    )
                    stmt = stmt.where(in_clause)

                # Print the statement for debugging purposes
                print(stmt)

                #       Execute the query
                result = self.execute_query(stmt, True, update=[tag])
                self.engine.commit()

            print("Update successful")
            return True

        except Exception as e:
            # Logging an exception if any error occurs during the process
            print(f"Error updating data: {e}")
            return False

        finally:
            # Ensure that the connection is closed
            if "conn" in locals() and self.engine:
                self.engine.close()

    def insert_dict(self, data, table):
        """
        Insert dictionary into a SQL database table.

        Args:
            data (dict): The dictionary containing column names and values to insert.
            table (str): The table in which the records should be inserted.

        Returns:
            (bool) True if successfully inserted, else False.
        """
        print(f"Inserting dictionary data into `{table}`...")
        print("testing")
        print(f"Data:\n{data}")

        try:

            # metadata = MetaData()
            # metadata.bind = self.engine  # Bind the metadata to the engine
            # table = Table(table, metadata, autoload_with=self.engine)
            table = self.tables.get(table)

            if not isinstance(data, list):
                data = [data]

            stmt = insert(table).values(data)

            # Executing the constructed query with the parameters
            result = self.execute_query(stmt, True, insert=self.fetch_schemas())
            self.engine.commit()

            print("Insert successful")
            return True

        except Exception as e:
            # Logging an exception if any error occurs during the insertion process
            print(f"Error inserting data: {e}")
            return False
        finally:
            # Ensure that the cursor and connection are closed
            if "conn" in locals() and self.engine:
                self.engine.close()

    def update_audit(self, data, table):
        """
        Insert dictionary into a SQL database table.

        Args:
            data (dict): The dictionary containing column names and values to insert.
            table (str): The table in which the records should be inserted.

        Returns:
            (bool) True if successfully inserted, else False.
        """
        print(f"Inserting dictionary data into `{table}`...")
        print("testing")
        print(f"Data:\n{data}")

        try:
            # Create connection string
            connection_string = f"{self.DB_TYPE}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"

            # Establish database connection
            conn = psycopg2.connect(connection_string)
            cursor = conn.cursor()

            # Extracting column names and values
            column_names = [f'"{column_name}"' for column_name in data.keys()]
            params = list(data.values())

            print(f"Column names: {column_names}")
            print(f"Params: {params}")

            # Constructing the SQL query dynamically
            columns_string = ", ".join(column_names)
            param_placeholders = ", ".join(["%s"] * len(column_names))
            print(
                f"Columns inserting: {columns_string} and values: {param_placeholders}"
            )

            # Making the Insert query
            query = (
                f"INSERT INTO {table} ({columns_string}) VALUES ({param_placeholders})"
            )

            # Executing the constructed query with the parameters
            cursor.execute(query, params)
            conn.commit()

            print("Insert successful")
            return True
        except Exception as e:
            # Logging an exception if any error occurs during the insertion process
            print(f"Error inserting data: {e}")
            return False
        finally:
            # Ensure that the cursor and connection are closed
            if "cursor" in locals() and cursor:
                cursor.close()
            if "conn" in locals() and conn:
                conn.close()

    def log_error_to_db(self, data, table):
        """
        Insert dictionary into a SQL database table.

        Args:
            data (dict): The dictionary containing column names and values to insert.
            table (str): The table in which the records should be inserted.

        Returns:
            (bool) True if successfully inserted, else False.
        """
        print(f"Inserting dictionary data into `{table}`...")
        print("testing")
        print(f"Data:\n{data}")

        try:
            # Create connection string
            connection_string = f"{self.DB_TYPE}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"

            # Establish database connection
            conn = psycopg2.connect(connection_string)
            cursor = conn.cursor()

            # Extracting column names and values
            column_names = [f'"{column_name}"' for column_name in data.keys()]
            params = list(data.values())

            print(f"Column names: {column_names}")
            print(f"Params: {params}")

            # Constructing the SQL query dynamically
            columns_string = ", ".join(column_names)
            param_placeholders = ", ".join(["%s"] * len(column_names))
            print(
                f"Columns inserting: {columns_string} and values: {param_placeholders}"
            )

            # Making the Insert query
            query = (
                f"INSERT INTO {table} ({columns_string}) VALUES ({param_placeholders})"
            )

            # Executing the constructed query with the parameters
            cursor.execute(query, params)
            conn.commit()

            print("Insert successful")
            return True
        except Exception as e:
            # Logging an exception if any error occurs during the insertion process
            print(f"Error inserting data: {e}")
            return False
        finally:
            # Ensure that the cursor and connection are closed
            if "cursor" in locals() and cursor:
                cursor.close()
            if "conn" in locals() and conn:
                conn.close()

    def insert(
        self, data, table, database=None, if_exists="replace", index=False, method=None
    ):
        """
        Write records stored in a DataFrame to a SQL database.

        Args:
            data (DataFrame): The DataFrame that needs to be write to SQL database.
            table (str): The table in which the rcords should be written to.
            kwargs: Keyword arguments for pandas to_sql function.
                See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
                to know the arguments that can be passed.

        Returns:
            (bool) True is succesfully inserted, else false.
        """
        # print(f'Inserting into `{table}`')

        try:
            data.to_sql(
                table, self.engine, if_exists=if_exists, index=index, method=method
            )
            return True
        except:
            print("exception")
            return False

    def insert_data(self, data, table):
        """
        Insert dictionary into a SQL database table and return the inserted ID.

        Args:
            data (dict): The dictionary containing column names and values to insert.
            table (str): The table in which the records should be inserted.

        Returns:
            (int) The ID of the inserted row if successful, else None.
        """
        print(f"Inserting dictionary data into `{table}`...")
        print("testing")
        print(f"Data:\n{data}")

        try:
            # Get the table object from metadata
            table = self.tables.get(table)

            if not isinstance(data, list):
                data = [data]

            # Assuming the table has an 'id' column to return after insert
            stmt = insert(table).values(data).returning(table.c.id)

            # Executing the constructed query
            result = self.engine.execute(stmt)
            inserted_id = result.scalar()

            # Committing the transaction
            self.engine.commit()

            print(f"Insert successful, inserted ID: {inserted_id}")
            return inserted_id

        except Exception as e:
            # Logging any error that occurs during the insertion process
            print(f"Error inserting data: {e}")
            return None

        finally:
            # Ensure that the connection is closed
            if "conn" in locals() and self.engine:
                self.engine.close()

    def get_table_columns(self, table_name):
        query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        return pd.read_sql(query, self.engine)

    def get_columns(self, table):
        """
        Get all column names from an SQL table.

        Args:
            table (str): Name of the table from which column names should be extracted.
            database (str): Name of the database in which the table lies. Leave
                it none if you want use database during object creation.

        Returns:
            (list) List of headers. (None if an error occurs)
        """
        try:
            print(f"Getting column names of table `{table}`")
            # Query to fetch column names from the table
            return list(self.execute_query(f"SELECT * FROM `{table}`"), True)
        except:
            print("Something went wrong getting column names. Check trace.")
            return False

    def execute_default_index(self, query, **kwargs):
        """
        Executes an SQL query.

        Args:
            query (str): The query that needs to be executed.
            database (str): Name of the database to execute the query in. Leave
                it none if you want use database during object creation.
            params (list/tuple/dict): List of parameters to pass to in the query.

        Returns:
            (DataFrame) A pandas dataframe containing the data from the executed
            query. (None if an error occurs)
        """
        data = None

        try:
            # Execute the SQL query using pandas read_sql function
            data = pd.read_sql(query, self.engine, **kwargs).replace({pd.np.nan: None})
        except exc.ResourceClosedError:
            # Handle the case where the query is executed successfully but returns no data
            return True
        except:
            # Log and handle any other exceptions that occur during query execution
            print(f"Something went wrong while executing query. Check trace.")
            params = kwargs["params"] if "params" in kwargs else None
            return False

        return data.where((pd.notnull(data)), None)
