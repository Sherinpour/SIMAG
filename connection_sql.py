import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ---- Connection settings ----
server = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')

# ---- Create connection ----
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

try:
    conn = pyodbc.connect(conn_str)
    print("Connected successfully!")

    query = """
    SELECT [ID],
           [FirstName],
           [LastName],
           [BankTitle],
           [Post],
           [OrganizationTitle],
           [OrganizationTypeTitle],
           [CompanyTitle],
           [HoldingTitle]
    FROM [GEMS].[dbo].[vw_Guest_AI]
    """

    df = pd.read_sql(query, conn)
    df.to_csv("output.csv", index=False, encoding="utf-8-sig")

except Exception as e:
    print("Error:", e)

finally:
    try:
        conn.close()
    except:
        pass
