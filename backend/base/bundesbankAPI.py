import requests
import os
import logging
import pandas as pd
import io
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DateProcessor(ABC):
    def __init__(self, df):
        super().__init__()
        self.df = df

    @abstractmethod
    def filter_date(self, start_date= None, end_date= None):
        pass

class MonthlyProcessor(DateProcessor):
    def filter_date(self, start_date=None, end_date=None):
        self.df["TIME_PERIOD"] = pd.to_datetime(self.df["TIME_PERIOD"], format="%Y-%m")
        if start_date:
            self.df = self.df[self.df['TIME_PERIOD'] >= pd.to_datetime(start_date)]
        if end_date:
            self.df = self.df[self.df['TIME_PERIOD'] <= pd.to_datetime(end_date)]
        logging.info(f"Filtered Data from: {start_date} to {end_date}")
        return self.df


class BundesbankAPI:
    BASE_URL = "https://api.statistiken.bundesbank.de/rest"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"}
        )

    def get_csv_data(self, flow_ref, key):
        url = f"{self.BASE_URL}/data/{flow_ref}/{key}"
        logger.info(f"Retrieving time series data: flow_ref={flow_ref}, key={key}")
        try:
            response = self.session.get(url, headers=self.session.headers)
            response.raise_for_status()  
            self.df = pd.read_csv(io.StringIO(response.text), sep=';', na_values=['.'])
            self.df.to_csv("test.csv")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error retrieving time series data: {e}")
            raise
    
    def process_date(self, start_date=None, end_date=None):
        time_format = self.df["TIME_FORMAT"][0]

        # Map time formats to processors
        processor_map = {
            "P1M": MonthlyProcessor,
        
        #TODO: Add more date format
        }

        processor_class = processor_map.get(time_format)
        if processor_class is None:
            raise ValueError(f"Unsupported time format: {time_format}")

        processor = processor_class(self.df)
        self.df = processor.filter_date(start_date=start_date, end_date=end_date)
        # self.df.to_csv("filtered_data.csv")
        return self.df

    def get_data(self):
        return self.df[["TIME_PERIOD", "OBS_VALUE"]]

if __name__ == "__main__":
    api = BundesbankAPI()
    # api.get_csv_data("BBFBOPV", "M.N.DE.W1.S1.S1.T.C.G._Z._Z._Z.EUR._T._X.N.ALL")
    # api.process_date("1971-01", "1971-03")
    api.get_csv_data("BBEX3", "M.ISK.EUR+USD.CA.AC.A01")

    #TODO: Generate ID based on input