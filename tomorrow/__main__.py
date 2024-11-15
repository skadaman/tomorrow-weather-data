import logging
import os
import sys
import datetime as dt
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from tomorrow import api_key#, weather_locations
from tomorrow.api import APIData
import pytz
from tomorrow.db import get_connection, get_weather_locations, upsert_weather_data

# configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Starting data pull from Tomorrow.io ")

now = dt.datetime.now(pytz.UTC)
future_end_time = (now + dt.timedelta(days=+5)).isoformat()

api = APIData(api_key, logger) 
try:
   db_conn =  get_connection()
   logger.info(f"Getting all locations")
   weather_locations = get_weather_locations(db_conn)
except Exception as e:
    logger.error(f"Error getting locations: {str(e)}")
    raise
    
finally:
    if db_conn:
        db_conn.close()

# Get data for all locations
for i in weather_locations.keys():
    logger.info(f"Starting Data process for location {i}")
    location = weather_locations[i] 
    # Get historical data and store in dict. 
    logger.info(f"Pulling Historical Data")
    hist_data = api.get_hist_recent_data(location)

    # Get forecast data and store in dict. 
    logger.info(f"Pulling Historical Data")
    forecast_data = api.get_forecast_data(location, now, future_end_time)
    try:
        # Get database connection
        db_conn = get_connection()
        logger.info(f"Upserting historical data")
        upsert_weather_data(hist_data, i, db_conn, "historical_weather")
        logger.info(f"Upserting forecast data")
        upsert_weather_data(forecast_data, i, db_conn, "forecast_weather")
        
    except Exception as e:
        logger.error(f"Error processing weather data: {str(e)}")
        raise
        
    finally:
        if db_conn:
            db_conn.close()





