import os
from google.cloud.sql.connector import Connector
import sqlalchemy
import pandas as pd

# Set the environment variable for authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\Courses\Python\Pycharm Fun projects\Streamlit\Streamlit_GCP_SQL\.streamlit\mysql-cloud-427906-9c20e22716fa.json"

# Initialize the connector
connector = Connector()

# Define the connection
def getconn():
    conn = connector.connect(
        "mysql-cloud-427906:asia-south1:cloud-sql-demo",
        "pymysql",
        user="root",
        password="7869816602",
        db="banking"
    )
    return conn

# Use SQLAlchemy for connection pooling
engine = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn
)

# Example: Execute a query and return a DataFrame
def query_to_dataframe(query):
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df

# Example usage
df = query_to_dataframe("SELECT * FROM Bank;")
print(df)
