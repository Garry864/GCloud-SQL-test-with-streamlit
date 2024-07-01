import streamlit as st
import os
from google.cloud.sql.connector import Connector
import sqlalchemy
import pandas as pd

# Load secrets directly from st.secrets
gcp_service_account = st.secrets["gcp_service_account"]
database_secrets = st.secrets["database"]

# Set Google Cloud credentials environment variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json.dumps({
    "type": gcp_service_account["type"],
    "project_id": gcp_service_account["project_id"],
    "private_key_id": gcp_service_account["private_key_id"],
    "private_key": gcp_service_account["private_key"],
    "client_email": gcp_service_account["client_email"],
    "client_id": gcp_service_account["client_id"],
    "auth_uri": gcp_service_account["auth_uri"],
    "token_uri": gcp_service_account["token_uri"],
    "auth_provider_x509_cert_url": gcp_service_account["auth_provider_x509_cert_url"],
    "client_x509_cert_url": gcp_service_account["client_x509_cert_url"]
})

# Use Google Cloud Storage API
client = storage.Client()

# Example usage
bucket = client.get_bucket("your-bucket-name")

# Initialize the connector
connector = Connector()

# Define the connection function
def getconn():
    conn = connector.connect(
        database_secrets["INSTANCE_CONNECTION_NAME"],
        "pymysql",
        user=database_secrets["DB_USER"],
        password=database_secrets["DB_PASS"],
        db=database_secrets["DB_NAME"]
    )
    return conn

# Use SQLAlchemy for connection pooling
engine = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn
)

# Function to execute a query and return a DataFrame
def query_to_dataframe(query):
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

# Function to execute a stored procedure and return a DataFrame
def execute_stored_procedure(proc_name, *args):
    with engine.connect() as connection:
        # Create a cursor object
        cursor = connection.connection.cursor()
        # Call the stored procedure
        cursor.callproc(proc_name, args)
        # Fetch the results
        results = cursor.fetchall()
        # Fetch column names
        columns = [desc[0] for desc in cursor.description]
        # Close the cursor
        cursor.close()
        # Convert to DataFrame
        df = pd.DataFrame(results, columns=columns)
    return df

# Streamlit app
st.title("Google Cloud SQL with Streamlit")

# Text area for SQL query
query = st.text_area("Enter your SQL Query", "SELECT * FROM Bank;")

# Text input for stored procedure name
proc_name = st.text_input("Stored Procedure Name", "paystatement")

# Text inputs for stored procedure arguments
args = st.text_input("Stored Procedure Arguments (comma separated)", "2, 1000000002")

# Button to execute query
query_button = st.button("Execute Query", key='query_button')

# Button to execute stored procedure
proc_button = st.button("Execute Stored Procedure", key='proc_button')

if query_button:
    # Execute query to get results as a DataFrame
    df = query_to_dataframe(query)
    # Display the DataFrame in Streamlit
    st.dataframe(df)

if proc_button:
    # Split the arguments by comma and convert to a list of integers
    proc_args = [int(arg.strip()) for arg in args.split(',')]
    # Execute stored procedure to get results as a DataFrame
    df = execute_stored_procedure(proc_name, *proc_args)
    # Display the DataFrame in Streamlit
    st.dataframe(df)

# Close the connector
connector.close()
