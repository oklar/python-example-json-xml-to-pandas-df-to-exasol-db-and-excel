import pandas as pd
from pyexasol import ExaConnection


class ExasolDataPusher:
    def __init__(self, connection: ExaConnection):
        self.connection = connection

    def push_data(self, data: pd.DataFrame, table_name: str):
        self.connection.import_from_pandas(data, table_name)
