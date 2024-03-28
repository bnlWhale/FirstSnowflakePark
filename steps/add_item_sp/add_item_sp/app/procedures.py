from __future__ import annotations

import sys

from common import print_hello
from snowflake.snowpark import Session





def hello_procedure(session: Session, num: int, name: str) -> str:
    return print_hello(name, num)



# For local debugging
# Beware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.config("local_testing", True).getOrCreate() as session:
        print(hello_procedure(session, count, sys.argv[1]))  # type: ignore
