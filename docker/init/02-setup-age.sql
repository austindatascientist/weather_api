-- Enable Apache AGE extension
CREATE EXTENSION IF NOT EXISTS age;

-- Load AGE into the search path
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

-- Create the weather graph
SELECT create_graph('weather_graph');
