-- Create locations table
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    geojson JSONB NOT NULL
);

-- Create index on geojson field for better query performance
CREATE INDEX idx_locations_geojson ON locations USING GIN (geojson);

-- Create historical weather table
CREATE TABLE historical_weather (
    id INTEGER REFERENCES locations(id) ON DELETE CASCADE,
    datetime TIMESTAMP NOT NULL,
    field VARCHAR(100) NOT NULL,
    value NUMERIC NOT NULL,
    --update_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, datetime, field)
);

-- Create indexes for common query patterns
CREATE INDEX idx_historical_weather_datetime ON historical_weather(datetime);
CREATE INDEX idx_historical_weather_field ON historical_weather(field);

-- Create forecast weather table
CREATE TABLE forecast_weather (
    id INTEGER REFERENCES locations(id) ON DELETE CASCADE,
    datetime TIMESTAMP NOT NULL,
    field VARCHAR(100) NOT NULL,
    value NUMERIC NOT NULL,
    --update_stamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, datetime, field)
);

-- Create indexes for common query patterns
CREATE INDEX idx_forecast_weather_datetime ON forecast_weather(datetime);
CREATE INDEX idx_forecast_weather_field ON forecast_weather(field);

-- Insert location data
INSERT INTO locations (id, geojson) VALUES 
(1, '{"type": "Point", "coordinates": [-97.4200, 25.8600]}'),
(2, '{"type": "Point", "coordinates": [-97.5200, 25.9000]}'),
(3, '{"type": "Point", "coordinates": [-97.4800, 25.9000]}'),
(4, '{"type": "Point", "coordinates": [-97.4400, 25.9000]}'),
(5, '{"type": "Point", "coordinates": [-97.4000, 25.9000]}'),
(6, '{"type": "Point", "coordinates": [-97.3800, 25.9200]}'),
(7, '{"type": "Point", "coordinates": [-97.5400, 25.9400]}'),
(8, '{"type": "Point", "coordinates": [-97.5200, 25.9400]}'),
(9, '{"type": "Point", "coordinates": [-97.4800, 25.9400]}'),
(10, '{"type": "Point", "coordinates": [-97.4400, 25.9400]}');

-- Set the sequence to start after our inserted IDs
SELECT setval('locations_id_seq', 10);
