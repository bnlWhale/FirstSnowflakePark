from snowflake.snowpark import Session


class Count_obj:
    run_times = 1
    session = None

    def __init__(self, run_times):
        self.run_times = run_times
        self.session = Session.builder.getOrCreate()
