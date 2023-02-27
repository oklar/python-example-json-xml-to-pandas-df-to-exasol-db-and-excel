import asyncio

from data_processor import DataProcessor

if __name__ == '__main__':
    processor = DataProcessor(
        url_temperature='',
        url_humidity='',
        table_name='',
        exasol_dsn='',
        exasol_user='',
        exasol_password=''
    )
    asyncio.run(processor.process_data())
