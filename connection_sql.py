import pyodbc
import pandas as pd

# ---- Connection settings ----
server = '172.17.19.3'
database = 'GEMS'
username = 'vw_user_ai'
password = '987654321'

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
