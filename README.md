### TOMORROW.IO DATA SCRAPPER ###
The code in this can be use to pull data from tomorrow.io's api. Currently two endpoints are supported:

1. https://docs.tomorrow.io/reference/weather-recent-history
2. https://docs.tomorrow.io/reference/weather-forecast

These endpoints can be accessed through their free plan. 

The main routine will pull the historical weather data for the previous day, and forecast data for the next five days. This is relative to the day the routine is executed. 

The data is pulled for 10 locations:
|   lat   |   lon    |
|:-------:|:--------:|
| 25.8600 | -97.4200 |
| 25.9000 | -97.5200 |
| 25.9000 | -97.4800 |
| 25.9000 | -97.4400 |
| 25.9000 | -97.4000 |
| 25.9200 | -97.3800 |
| 25.9400 | -97.5400 |
| 25.9400 | -97.5200 |
| 25.9400 | -97.4800 |
| 25.9400 | -97.4400 |

To execute the system users just need to run `docker compose up --build` and can clean up the system with `docker compose down -v`

The python notebook is accesible through [localhost:8888](http://localhost:8888). 

## Database Information
The data is then stored in a postgress sql db with the following schema:
![alt text](image.png)

The database structure was setup in a long format (a field name and a value column) so that if the fields that are pull can change without having to change the schema of the table. Additionally, if in the future the different types of data were required to be split out into their own tables then that migration would be straight forward with this format. Although the dimensionality of the historical weather table and the forecasted weather table is the same the data was kept in separate tables because it comes from different sources. If at any point the endpoint where to change or a new schema was required for the different kinds of data then this approach is more robust to changes.

Indexes are set up envisioning what frequent querying of these tables could look like. For example querying the data by date is optimized by having these indexes.

Locations are saved in geojson as that is the best way to store location data. Having the data in this format would lend itself to some other applications such as mapping.

## Python API Code

To do database CRUD operations I am using a psycopg2 and pdSQL because the operations are relatively simple and won't need to be maintained overtime. With more time a more sophisticated ORM approach could have been taken. The queries to insert weather data are set up as UPSERTS. That way if forecasts or historical records are updated then so will the entries in the database. 

The code flow can be summarized in the following steps:

1. Pull db credentials and the API key from env variables. 
2. Create a db connection and pull all the locations in the locations table. 
3. Iterate through each location.
    a. Pull historical weather from tomorrow.io
    b. Pull forecasted weather from tomorrow.io
    c. UPSERT the data into the db.

I acknowledge that saving API keys in repos is not best practice. The api key should be saved in a secret manager such as EnvKey or AKV then passed as env variable.

The db functions are set up as standalone functions. I could have set that up as its own class and then had the API class inherit the db class. This would make the getting of the `db_conn` object sightly cleaner but for this quick use case that seemed like over kill. Additionally, I will use the functions are used in the python notebook and so we don't have to initialize a class there as well. 

The APIData class is meant to facilitate the API calls. By storing the urls and other constants as objects for the class we can keep the code clean and only update the code in a single place, for example an API url were to change. This design was also meant to facilitate the passing of the API key across functions. 

I wrap all db connections and API calls in try-catch statements so that we can handle those exceptions coming from third parties cleanly. In the db case we can rollback changes and so forth. Additionally, I saw the hint to use pytest. For this case I opted to use a custom function to check for data quality rather than using the pytest framework.  

To keep the code simple I am just using a for-loop to iterate through the different locations. However, this could would be easily adaptable to a async multi-threaded approach where the data for each node could be pulled in parallel. Since the data we are pulling is not very big and the api key is limited to 3 requests per second that seemed like overkill. 

## Python Notebook

The python notebook is very bare bones, but I believe the point of the exercise is to demonstrate a system with a front end and a back end and the flow of data between those. I think the notebook has enough to accomplish that. 


Users should be able to get to a simple time series plot with some interactive drop downs that allow them to pick the field and geolocation of interest. The plots could be further enhanced by adding some labels that match the corresponding lat,longs to the nearest city so that the plot were more informative. 

Users could potentially use the query function in the notebook to submit any select query if they wanted to do any more data exploration or aggregation. The generalized function for queries gives the notebook a bit more power than just the plot displayed.
