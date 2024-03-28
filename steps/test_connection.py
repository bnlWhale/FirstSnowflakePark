from snowflake.snowpark import Session


# For local debugging
if __name__ == "__main__":
    # Create a local Snowpark session
    print(__name__)
    with Session.builder.getOrCreate() as session:
        print("get session")

