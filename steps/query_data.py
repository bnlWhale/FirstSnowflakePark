import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from thead_snowpark.loop_snowpark import Count_obj
import time, threading
from steps.thead_snowpark.loop_snowpark import Count_obj

count_obj = Count_obj(3)


def add_order_thread():
    count = 1
    while count <= count_obj.run_times:
        print("add order thread:", count)
        add_orders(count_obj.session)
        count += 1
        time.sleep(1)


def consume_orders_thread():
    count = 1
    while count <= count_obj.run_times:
        print("consume order thread:", count)
        consumer_orders(count_obj.session)
        count += 1
        time.sleep(1.5)


def main(session: Session):
    # session.sql("insert into HOL_DB.SHOPONLINE.ITEM values ( 1, 'running shoe', null, '2020-04-04 16:57:53.653')").collect()
    # session.sql("update HOL_DB.SHOPONLINE.ITEM set id = 2 where name = 'walking shoe'").collect()
    # session.sql('select * from HOL_DB.SHOPONLINE.STORAGE').show()
    # session.call("HELLO_PROCEDURE", num, name)
    # name = 'matt'
    # print(session.sql(
    #     "insert into HOL_DB.SHOPONLINE.STORAGE (id, name, quantity) values (1, 'running shoe', 100)").collect())
    # print(session.sql(
    #     "insert into HOL_DB.SHOPONLINE.STORAGE (id, name, quantity) values (2, 'walking shoe', 100)").collect())
    # print(session.sql(
    #     "insert into HOL_DB.SHOPONLINE.ORDER_SHOP (item_id, id_user, amount, created_at) values (2, 10001, 1, current_timestamp())").collect())
    # create_orders_stream(session)
    # create_user_table(session)
    # print(session.sql(
    #     "insert into HOL_DB.SHOPONLINE.USERS (id, name, created_at) values (10001, 'Trump', current_timestamp())").collect())
    # monitor_order_updates(session)
    # create_table_dummy(session)
    # create_orders(session)
    # create_delivery(session)
    # create_tasks(session)

    # will be used thread executor
    td = threading.Thread(target=add_order_thread, name="add_order_thread")
    td.start()
    # td.join()
    td = threading.Thread(target=consume_orders_thread, name="add_order_thread")
    td.start()
    # td.join()

    time.sleep(6)
    print("main execution end")


def create_tasks(session: Session):
    print("creating tasks")
    # session.sql("create or replace task payment_task warehouse=HOL_WH as insert into HOL_DB.SHOPONLINE.HAPPYPAYMENT(order_id, user_id, amount, created_at)  select id, id_user, amount, current_timestamp() from HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM").show()
    # session.sql("alter task payment_task RESUME").show()
    # session.sql("show tasks in HOL_DB.SHOPONLINE").show()
    session.sql("execute task HOL_DB.SHOPONLINE.payment_task").show()


def stop_all_tasks(session: Session):
    print("stopped all tasks")


def create_coupons(session: Session):
    session.sql(
        "CREATE TABLE HOL_DB.SHOPONLINE.HAPPYCOUPONS( id integer autoincrement, user_id integer, points integer, created_at timestamp)").collect()


def create_delivery(session: Session):
    session.sql(
        "CREATE TABLE HOL_DB.SHOPONLINE.HAPPYDELIVERY( id integer autoincrement, order_id integer, states integer, created_at timestamp)").collect()


def create_payment(session: Session):
    session.sql(
        "CREATE TABLE HOL_DB.SHOPONLINE.HAPPYPAYMENT( id integer autoincrement, order_id integer, user_id integer, amount integer, created_at timestamp)").collect()


def add_orders(session: Session):
    print(session.sql(
        "insert into HOL_DB.SHOPONLINE.ORDER_SHOP (item_id, id_user, amount, created_at) values (2, 10001, 1, current_timestamp())").collect())


def consumer_orders(session: Session):
    print(session.sql(
        "create or replace task payment_task warehouse=HOL_WH as insert into HOL_DB.SHOPONLINE.HAPPYPAYMENT(order_id, user_id, amount, created_at)  select id, id_user, amount, current_timestamp() from HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM").show())


def create_orders_stream(session):
    _ = session.sql("CREATE STREAM HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM ON TABLE HOL_DB.SHOPONLINE.ORDER_SHOP").collect()


def create_table_dummy(session):
    session.sql("create table HOL_DB.SHOPONLINE.DUMMY(id integer, item_id integer, id_user integer, amount integer,"
                "created_at timestamp)").collect()


def create_table_user(session):
    session.sql(
        "CREATE TABLE HOL_DB.SHOPONLINE.USER( id integer autoincrement, name varchar (250), created_at timestamp").collect()


def monitor_order_updates(session):
    source = session.table('HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM')
    target = session.table('HOL_DB.SHOPONLINE.ORDER_SHOP')
    # source.show()
    # target.show()
    # session.sql("select * from HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM").show()
    cols_to_update = {c: source[c] for c in source.schema.names if "METADATA" not in c}
    # print(cols_to_update)
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}
    print(updates)
    # session.sql("insert into HOL_DB.SHOPONLINE.DUMMY(item_id, id_user, amount) select item_id, id_user, amount from HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM where false").collect()


# id, item_id, id_user, amount, created_at
# session.sql("select id, item_id, id_user, amount, created_at from HOL_DB.SHOPONLINE.ORDER_SHOP_STREAM").show()

if __name__ == '__main__':
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        import sys

        if len(sys.argv) > 1:
            main(session, *sys.argv[1:])  # type: ignore
        else:
            main(session)  # type: ignore
