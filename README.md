# Weather API

A containerized weather data ingestion and retrieval service. Fetches forecasts from weather.gov and models weather relationships using PostgreSQL with the Apache AGE extension.

**Stack:** Docker, FastAPI, PostgreSQL + Apache AGE, Airflow, pgAdmin

---

## Setup

**Docker Desktop must be running** before executing any make commands. Docker Desktop can be downloaded at [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/).

This project has been tested in a **Debian Windows Subsystem for Linux (WSL) + Visual Studio Code** environment. To set up this environment:
1. Download [Visual Studio Code](https://code.visualstudio.com/Download) for Windows
2. Install [Debian from the Microsoft Store](https://apps.microsoft.com/detail/9msvkqc78pk6)
3. Install the [WSL extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-wsl) in VS Code

1. **Clone the repository:**
   ```bash
   git clone https://github.com/austindatascientist/weather_api.git
   cd weather_api
   ```

2. **Create environment file:**
   ```bash
   cp .env.example .env
   ```

3. **Start all services:**
   ```bash
   make up
   ```

4. **Fetch weather data:**
   ```bash
   make weather                  # Default: Huntsville, AL (auto-creates graph nodes)
   make weather "Denver"         # Custom location (auto-creates graph nodes)
   ```

**Bulk create weather data for top 10 populous US cities:**

```bash
make cities
```

---

## Services

All services start automatically with `make up`:

| Service | URL | Login |
|---------|-----|-------|
| **FastAPI** (Swagger UI) | http://localhost:8000/docs | - |
| **Airflow** (Workflow scheduler) | http://localhost:8080 | `admin` / `admin` |
| **pgAdmin** (Database UI) | http://localhost:5050 | `admin@example.com` / `admin` |

### pgAdmin Server Setup

After opening http://localhost:5050 with the login above:

1. Right-click **Servers** → **Register** → **Server...**
2. Configure connection:
   - **General tab:** Name: `weather-server`
   - **Connection tab:**
     - Host: `postgres`
     - Port: `5432`
     - Database: `weather`
     - Username: `postgres`
     - Password: `postgres`
3. Click **Save**

Open Query Tool: Right-click **weather** database → **Query Tool** (or `Alt+Shift+Q`)

---

## Basic Usage

### PostgreSQL Queries

Run in pgAdmin Query Tool (F5 to execute):

```sql
-- View all weather data
SELECT * FROM weather_data ORDER BY date;
```

```sql
-- Find hottest days across all locations
SELECT location_name, date, high_temp_f
FROM weather_data
WHERE high_temp_f = (SELECT MAX(high_temp_f) FROM weather_data);
```

### Backups & Restore

```bash
make backup                   # Manual backup to ./backups
make restore                  # Restore from latest backup
```

Automated backups run daily at 3:00 AM.

### Docker Management

```bash
make restart                  # Full restart
make down                     # Stop and remove all containers
```

---

## Weather Relationship Graph

Nodes such as Temperature and Humidity are connected by edges representing weather patterns and correlations. This enables queries to discover how weather variables influence each other across locations.

## Apache AGE Extension Graph Queries

**⚠️ IMPORTANT:** All Cypher queries require Apache AGE setup. Run these commands **once per Query Tool window**:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
```

Then run your Cypher queries. All examples below include these setup commands.

### 1. Coastal Proximity & Temperature Moderation

Coastal cities have smaller temperature ranges. Per-city correlation between temperature and daily temperature variance (includes Pearson correlation coefficient):

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

WITH coast_temp AS (
    SELECT * FROM cypher('weather_graph', $$
        MATCH (l:Location), (t:Temperature)
        WHERE l.name = t.location
        RETURN l.name as city, l.distance_to_coast_km as coast_dist,
               t.value_f as temp, t.time_of_day as time_of_day,
               CASE WHEN l.distance_to_coast_km < 100 THEN 'coastal'
                    WHEN l.distance_to_coast_km < 300 THEN 'near_coast'
                    ELSE 'inland' END as region
    $$) as (city agtype, coast_dist agtype, temp agtype, time_of_day agtype, region agtype)
),
city_stats AS (
    SELECT
        city,
        coast_dist,
        region,
        count(*) as readings,
        avg(temp::text::float) as avg_temp,
        max(temp::text::float) - min(temp::text::float) as temp_range,
        stddev(temp::text::float) as temp_stddev
    FROM coast_temp
    GROUP BY city, coast_dist, region
)
SELECT
    city,
    round(coast_dist::text::float::numeric, 1) as coast_dist,
    region,
    readings,
    round(avg_temp::numeric, 1) as avg_temp,
    round(temp_range::numeric, 1) as temp_range,
    round(temp_stddev::numeric, 2) as temp_stddev
FROM city_stats
ORDER BY coast_dist::text::float;
```

### 2. Latitude & Temperature Correlation

Per-city correlation between temperature and humidity (includes Pearson correlation coefficient):

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

WITH lat_temp AS (
    SELECT * FROM cypher('weather_graph', $$
        MATCH (l:Location), (t:Temperature)-[:CONCURRENT_WITH]->(h:Humidity)
        WHERE l.name = t.location
        RETURN l.name as city, l.latitude as lat, t.value_f as temp, h.value_percent as humidity
    $$) as (city agtype, lat agtype, temp agtype, humidity agtype)
)
SELECT
    city,
    round(lat::text::float::numeric, 2) as lat,
    count(*) as readings,
    round(avg(temp::text::float)::numeric, 1) as avg_temp,
    round(avg(humidity::text::float)::numeric, 1) as avg_humidity,
    round(corr(temp::text::float, humidity::text::float)::numeric, 3) as temp_humidity_corr
FROM lat_temp
GROUP BY city, lat
ORDER BY lat::text::float DESC;
```

### 3. Temperature vs Humidity Correlation by City

Per-city correlation between temperature and humidity readings:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

WITH temp_humidity AS (
    SELECT * FROM cypher('weather_graph', $$
        MATCH (t:Temperature)-[:CONCURRENT_WITH]->(h:Humidity)
        RETURN t.location as city, t.value_f as temp, h.value_percent as humidity
    $$) as (city agtype, temp agtype, humidity agtype)
)
SELECT
    city,
    count(*) as readings,
    round(avg(temp::text::float)::numeric, 1) as avg_temp,
    round(avg(humidity::text::float)::numeric, 1) as avg_humidity,
    round(corr(temp::text::float, humidity::text::float)::numeric, 3) as temp_humidity_corr
FROM temp_humidity
GROUP BY city
ORDER BY temp_humidity_corr;
```

### 4. Weather Similarity Between Cities

Compare a city's average temperature against all other cities (change `'Dallas'` to any city in your graph):

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

WITH base_city AS (
    SELECT * FROM cypher('weather_graph', $$
        MATCH (t:Temperature)
        WHERE t.location = 'Dallas'
        RETURN t.location as city, avg(t.value_f) as avg_temp, count(t) as readings
    $$) as (city agtype, avg_temp agtype, readings agtype)
),
other_cities AS (
    SELECT * FROM cypher('weather_graph', $$
        MATCH (t:Temperature)
        WHERE t.location <> 'Dallas'
        RETURN t.location as city, avg(t.value_f) as avg_temp, count(t) as readings
    $$) as (city agtype, avg_temp agtype, readings agtype)
)
SELECT
    'Dallas' as base_city,
    o.city as compare_city,
    round(b.avg_temp::text::float::numeric, 1) as dallas_avg_temp,
    round(o.avg_temp::text::float::numeric, 1) as other_avg_temp,
    round(abs(b.avg_temp::text::float - o.avg_temp::text::float)::numeric, 1) as temp_diff,
    o.readings
FROM base_city b, other_cities o
ORDER BY temp_diff;
```

