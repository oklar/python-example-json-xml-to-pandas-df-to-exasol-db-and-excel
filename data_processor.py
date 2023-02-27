import asyncio
import pandas as pd
import pyexasol

from data_fetcher import TemperatureJsonDataFetcher, HumidityXmlDataFetcher
from data_pusher import ExasolDataPusher


class DataProcessor:
    def __init__(self, url_temperature: str, url_humidity: str, table_name: str, exasol_dsn: str,
                 exasol_user: str, exasol_password: str):
        self.url_temperature = url_temperature
        self.url_humidity = url_humidity
        self.table_name = table_name
        self.exasol_dsn = exasol_dsn
        self.exasol_user = exasol_user
        self.exasol_password = exasol_password

    async def process_data(self):
        temp_data_fetcher = TemperatureJsonDataFetcher()
        hum_data_fetcher = HumidityXmlDataFetcher()
        exasol_pusher = ExasolDataPusher(
            pyexasol.connect(dsn=self.exasol_dsn, user=self.exasol_user, password=self.exasol_password))

        while True:
            temp_data_task = asyncio.create_task(temp_data_fetcher.fetch_data(self.url_temperature))
            hum_data_task = asyncio.create_task(hum_data_fetcher.fetch_data(self.url_humidity))
            temp_data, hum_data = await asyncio.gather(temp_data_task, hum_data_task)

            temp_df = pd.DataFrame(temp_data, columns=['Date', 'Temperature'])
            hum_df = pd.DataFrame(hum_data, columns=['Date', 'Humidity'])

            merged_df = pd.merge(temp_df, hum_df, on='Date')

            exasol_pusher.push_data(merged_df, self.table_name)

            temp_df['Mean'] = temp_df['Temperature'].expanding().mean()
            hum_df['Mean'] = hum_df['Temperature'].expanding().mean()

            temp_df.to_excel(f'{str(temp_df["Date"].min())}_{str(temp_df["Date"].min())}_temperature.xlsx', sheet_name="Temperature", index=False)
            hum_df.to_excel(f'{str(hum_df["Date"].min())}_{str(hum_df["Date"].min())}_humidity.xlsx', sheet_name="Humidity", index=False)

            await asyncio.sleep(10)
