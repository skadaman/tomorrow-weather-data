import os
import psycopg2 as pg
import pandas.io.sql as pdSQL
import logging 

logger = logging.getLogger(__name__)
# Bring in DB credentials from env
PGHOST = os.environ.get('PGHOST')
PGPORT = os.environ.get('PGPORT')
PGUSER = os.environ.get('PGUSER')
PGPASSWORD = os.environ.get('PGPASSWORD')
PGDATABASE = os.environ.get('PGDATABASE')

env_vars = [PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE ]

# Check for missing env values. 
if any([ v is None for v in env_vars]):
    raise("Missing DB credentials.")


# Function to get a db connection.
def get_connection():
    print("Connecting to DB")
    db_conn = pg.connect(database = PGDATABASE, 
                         user = PGUSER,
                         password = PGPASSWORD, 
                         host = PGHOST, 
                         port = PGPORT )

    return db_conn

''' Function to run data pulling queries, returns data frames from queried tables.'''
def execute_data_query(db_conn, query):
    logger = logging.getLogger(__name__)
    logger.info("Running Database Query")
    try:
        queryDF = pdSQL.read_sql_query(query, db_conn)
        return queryDF
    except pg.Error as e:
        logger.error(e)
        db_conn.rollback()  # Roll back in case of error

""" Function to UPSERT weather data to either of the weather tables."""
def upsert_weather_data(df, location_id, db_conn, table):

    # Convert DataFrame to list of tuples for faster insertion
    records = [
        (location_id, row['datetime'], row['field'], row['value'])
        for idx, row in df.iterrows()
    ]
    
    # Create cursor
    cursor = db_conn.cursor()
    
    # Bulk insert using executemany
    insert_query = f"""
    INSERT INTO {table} (id, datetime, field, value)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id, datetime, field) 
    DO UPDATE SET 
        value = EXCLUDED.value
    """             
    try:
        cursor.executemany(insert_query, records)
        db_conn.commit()
        
        logger.info(f"Successfully inserted {len(records)} weather records for location {location_id}")
        
    except pg.Error as e:
        logger.error(f"Database error: {str(e)}")
        db_conn.rollback()
        raise
    
    finally:
        cursor.close()

def get_weather_locations(db_conn):
    """
    Pull locations from database and format into dictionary matching:
    {
        "1": {"lat": 25.8600, "lon": -97.4200},
        "2": {"lat": 25.9000, "lon": -97.5200},
        ...
    }
    """

    logger = logging.getLogger(__name__)
    try:
        cursor = db_conn.cursor()
        query = """
        SELECT id, geojson 
        FROM locations 
        ORDER BY id;
        """

        cursor.execute(query)
        rows = cursor.fetchall()
    except pg.Error as e:
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        cursor.close()
    
    # Format into dictionary
    locations = {}
    for row in rows:
        loc_id, geojson = row
        # Parse JSON string if needed
        if isinstance(geojson, str):
            geojson = json.loads(geojson)
            
        # GeoJSON coordinates are [longitude, latitude]
        lon, lat = geojson['coordinates']
        
        locations[str(loc_id)] = {
            "lat": round(lat, 4),  # Match original precision
            "lon": round(lon, 4)
        }
        
    logger.info(f"Retrieved {len(locations)} locations from database")
    return locations
       


