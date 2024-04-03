import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()
def read_table(table):
   
    with snowflake.connector.connect(
        user=os.getenv('snowflake_user'),
        password=os.getenv('snowflake_password'),
        account=os.getenv('snowflake_account'),
        warehouse=os.getenv('snowflake_warehouse'),
        database=os.getenv('snowflake_database'),
        schema=os.getenv('snowflake_schema')
    ) as conn:
        with conn.cursor() as cur:
            cur.execute( f'SELECT * FROM {table}')
            return cur.fetch_pandas_all()

def write_table(df, table):
   
    with snowflake.connector.connect(
        user=os.getenv('snowflake_user'),
        password=os.getenv('snowflake_password'),
        account=os.getenv('snowflake_account'),
        warehouse=os.getenv('snowflake_warehouse'),
        database=os.getenv('snowflake_database'),
        schema=os.getenv('snowflake_schema')
    ) as conn:
        write_pandas(conn, df = df, 
            table_name =  table, 
            auto_create_table = True,
            overwrite = True)
        
def run_query(query):
   
    with snowflake.connector.connect(
        user=os.getenv('snowflake_user'),
        password=os.getenv('snowflake_password'),
        account=os.getenv('snowflake_account'),
        warehouse=os.getenv('snowflake_warehouse'),
        database=os.getenv('snowflake_database'),
        schema=os.getenv('snowflake_schema')
    ) as conn:
        with conn.cursor() as cur:
            cur.execute( query)
            return cur.fetch_pandas_all()