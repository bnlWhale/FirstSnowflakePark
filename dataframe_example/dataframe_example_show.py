# .. Snowflake Snowpark Session Library
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, stddev, stddev_pop
# .. Snowflake Snowpark Datatypes
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType

# .. Snowflake Snowpark Transformation
from snowflake.snowpark.functions import col

# .. Snowflake Snowpark All Functions
import snowflake.snowpark.functions as f

# .. Snowflake Snowpark Window Function
from snowflake.snowpark import Window
from snowflake.snowpark.functions import row_number

# For local debugging
conn_config = {
    "account": "KFBGAMM-MJB61345",
    "user": "ducounau",
    "password": "ilike@Bnl91",
    "role": "ACCOUNTADMIN",
    "warehouse": "compute_wh",
    "database": "DASH_DB",
    "schema": "DASH_DB"
}

# %%
#Invoking Snowpark Session for Establishing Connection

session = Session.builder.configs(conn_config).create()

# %%
# Using Snowpark Session Sql to query data
# Follows lazily executed approach
#.. Will not query data for below on snowflake
# query_1 = conn.sql("select * from DASH_DB.DASH_SCHEMA.CAMPAIGN_SPEND limit 10")
# query_1.show(5)

#.. Creating Dataframe by specifying range or sequence
df_create = session.create_dataframe([(1,'one'),(2,"two")],schema = ["col_1","col_2"])
df_create_rng = session.range(1,100,3).to_df("col_a")
df_create.show()
list_row = df_create_rng.collect()
for row in list_row:
    print(row)

df = session.create_dataframe([[1, 2], [3, 4], [1, 4]], schema=["A", "B"])
df.agg(stddev(col("a"))).show()

