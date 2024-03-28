from snowflake.snowpark import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import sproc
import json

connection_parameters = json.load(open('connection.json'))
session = Session.builder.configs(connection_parameters).create()
session.sql_simplifier_enabled = True

session.add_packages(["snowflake-snowpark-python"])


def add_two_number(sp_session: Session, input_1: int, input_2: int) -> str:
    return input_1 + input_2


session.sproc.register(func=add_two_number, is_permanent=True, stage_location="@procedure", name="add_two_number",
                       replace=True)


def do_data_trasform(sp_session: Session, tablename: str, stored_table: str) -> str:
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    data = sp_session.table(tablename)
    data_df = data.to_pandas()

    data_df = data_df.iloc[1:7, :]

    data_df = sp_session.create_dataframe(data_df)
    data_df.write.mode('overwrite').save_as_table(stored_table)
    return "Sucess"


session.sproc.register(func=do_data_trasform, is_permanent=True, stage_location="@procedure", name="do_data_trasform",
                       replace=True)

from snowflake.snowpark import Session

# Snowpark configuration
sf_options = {
    "account": "your account name",
    "user": "Username",
    "password": "Password",
    "warehouse": "warehouse name",
    "database": "database name",
    "schema": "schema name",

}
sess = Session.builder.configs(sf_options).create()
# Writing sql code for the Python Stored procedure
sql = (
    "create or replace PROCEDURE Multiply_together_1(input_number_1 int , input_number_2 int) \n"
    "returns String \n "
    "language python \n "
    "runtime_version = '3.8' \n"
    "packages = ('snowflake-snowpark-python') \n "
    "handler = 'multipler' \n "
    "as \n "

    "$$ \n"
    "def multipler(sess,a,b):\n"
    "  Z=a*b \n"
    "  return Z \n "
    "$$;")

res = sess.sql(sql)
res.show()

sql_1 = "call Multiply_together_1(5,8);"
# res1 = sess.sql(sql_1)
# res1.show()

@sproc(name="my_prc", is_permanent=True, stage_location="@procedure",replace=True,packages=["snowflake-snowpark-python"])
def my_proc(Session: snowpark.Session, x:int, y:int)->int:
  return int(x) + int(y)


print("complete")
