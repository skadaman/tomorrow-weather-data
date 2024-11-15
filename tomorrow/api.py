import pytz
import requests
import pandas as pd
from datetime import datetime

""" This class contains the functions needed to pull data from tomorrow.io """

class APIData:
    """ Initializing some fixed values. """
    def __init__(self, key, logger):
        self.key = key
        self.units = "imperial"
        self.timesteps = "1h"
        self.historical_url = "https://api.tomorrow.io/v4/historical"
        self.historical_recent_url = "https://api.tomorrow.io/v4/weather/history/recent"
        self.forecast_url = "https://api.tomorrow.io/v4/weather/forecast"
        self.time_zone = "US/Mountain"
        self.data_fields = ["totalPrecipitationAccumulationAvg",
                            "windSpeedAvg",
	                        "windDirectionAvg",
	                        "windGustMax",
                            "temperatureAvg"
                            ]
        self.logger = logger

    """ Function to test all temperature values are within the valid range (-128°F to 134°F)"""
    @staticmethod
    def temperature_values_in_range(df):
        # Filter data to temp field
        temp_data = df[df['field'] == 'temperature']
        
        # Get any values outside the valid range (highest and lowest temps recorded.)
        invalid_temps = temp_data[
            (temp_data['value'] > 134.1) | 
            (temp_data['value'] < -128.6 )
        ]
        
        # Error message
        if not invalid_temps.empty:
            error_details = invalid_temps.apply(
                lambda row: f"ID: {row['id']}, DateTime: {row['datetime']}, Value: {row['value']}",
                axis=1
            ).values
            
            error_msg = "\nInvalid temperature values found:\n" + "\n".join(error_details)
            logger.warning(error_msg)

    """ A function which given a location and dates will pull historical weather data"""
    def get_historical_data(self, location, start_date, end_date):

        body = {"location": f"{location['lat']},{location['lon']}", 
                "fields": self.data_fields, 
                "units": self.units, 
                "timesteps": self.timesteps, 
                "startTime": str(start_date), 
                "endTime": str(end_date),
                "timezone":self.time_zone
                }

        try:
            response = requests.post(f'{self.historical_url}?apikey={self.key}', json=body)
            response.raise_for_status()  # Raise exception for bad status codes
            
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch historical data: {str(e)}")
            raise

        data = response.json()
        data = data["data"]["timelines"]["intervals"]
        df = pd.DataFrame()

        for item in data:
            df = pd.json_normalize(item)
        df.rename(columns={'startTime': 'Date'}, inplace=True)
        df.Date = pd.to_datetime(df['Date'], format='%Y-%m-%d:%HH:%MM')


    """ A function which given a location and dates will pull historical weather data"""

    def get_hist_recent_data(self, location):

        location_str = f"{location['lat']},{location['lon']}"
        try:
            response = requests.get(f'{self.historical_recent_url}?location={location_str}&units={self.units}&apikey={self.key}')
            response.raise_for_status()  # Raise exception for bad status codes
            
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch historical data: {str(e)}")
            raise

        data = response.json()
        data = data["timelines"]["hourly"]
        df = self.parse_data(data)
        # Check data for bad temps. 
        APIData.temperature_values_in_range(df)
        return df

    """ A function to pull forecasts for a given location and dates """
    def get_forecast_data(self, location, start_date, end_date):

        params = {"location": f"{location['lat']},{location['lon']}", 
                "fields": self.data_fields, 
                "units": self.units, 
                "timesteps": self.timesteps, 
                "startTime": str(start_date), 
                "endTime": str(end_date),
                "timezone": self.time_zone,
                "apikey": self.key
                }

        try:
            response = requests.get(self.forecast_url, params=params)
            response.raise_for_status()  # Raise exception for bad status codes
            
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch forecast data: {str(e)}")
            raise
        data = response.json()
        data = data["timelines"]["hourly"]
        df = self.parse_data(data)
        # Check data for bad temps.
        APIData.temperature_values_in_range(df)
        return df

    """Function to take response parse it into a dataframe"""
    def parse_data(self, data):
        df = pd.DataFrame()
        flattened_data = []

        for item in data:
            # Parse the timestamp
            timestamp = datetime.strptime(item['time'], '%Y-%m-%dT%H:%M:%SZ')
            
            # Iterate through each field and value in the 'values' dictionary
            for field, value in item['values'].items():
                flattened_data.append({
                    'datetime': timestamp,
                    'field': field,
                    'value': value
                })
        df = pd.DataFrame(flattened_data)

        return df

