from abc import ABC, abstractmethod
import pandas as pd
import aiohttp


class AbstractDataFetcher(ABC):
    @abstractmethod
    async def fetch_data(self, url: str) -> pd.DataFrame:
        pass


class TemperatureJsonDataFetcher(AbstractDataFetcher):
    async def fetch_data(self, url: str) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response_json = await response.json()
        df = pd.json_normalize(response_json)
        return df


class HumidityXmlDataFetcher(AbstractDataFetcher):
    async def fetch_data(self, url: str) -> pd.DataFrame:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response_xml = await response.text()
        df = pd.read_xml(response_xml)
        return df
