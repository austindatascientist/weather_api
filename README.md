# Weather API

A containerized weather data ingestion and retrieval service. Fetches forecasts from weather.gov and models weather relationships using PostgreSQL with the Apache AGE extension.

**Stack:** Docker, FastAPI, PostgreSQL + Apache AGE, Airflow, pgAdmin

---

## Setup

**Docker Desktop must be running** before executing any commands. Docker Desktop can be downloaded at [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/).

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
   make weather                  # Default: Huntsville, AL
   make weather "San Diego"      # Custom location
   ```

5. **Initialize weather node relationships (optional):**
   ```bash
   docker exec api python -m app.create_node_relationships
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

Temperature, Humidity, and Precipitation are modeled as nodes with edges representing weather patterns and correlations. This enables queries to discover how weather variables influence each other across locations.

### Relationship Types

Key relationships used in queries:

| Edge Type | Description | Example |
|-----------|-------------|---------|
| **COOLING_EFFECT** | Rain brings cooler temperatures | 0.5" rain → 62°F |
| **CONCURRENT_WITH** | Readings taken at same time/location | Temp + Humidity at noon |
| **HAS_WEATHER** | Location has weather readings | Huntsville → 75°F high |

### Creating the Graph

**Bulk create default cities:**

```bash
docker exec api python -m app.create_node_relationships
```

Creates nodes with relationship edges for Southeast US cities.

**Add a specific city:**

```bash
curl -X POST "http://localhost:8000/api/graph/cities/nodes" \
  -H "Content-Type: application/json" \
  -d '{"city_name": "Denver"}'
```

**Note:** Distance to coast is calculated automatically when adding cities, using coastal reference points along Atlantic, Gulf, and Pacific coasts.

---

## Apache AGE Extension Graph Queries

**⚠️ IMPORTANT:** All Cypher queries require Apache AGE setup. Run these commands **once per Query Tool window**:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;
```

Then run your Cypher queries. All examples below include these setup commands.

### 1. Temperature & Rain Correlation

Cooling effect of precipitation on temperature:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('weather_graph', $$
    MATCH (p:Precipitation)-[:COOLING_EFFECT]->(t:Temperature)
    RETURN p.location, p.timestamp, p.value_inches as rain, t.value_f as temp
    ORDER BY p.value_inches DESC LIMIT 10
$$) as (location agtype, timestamp agtype, rain agtype, temp agtype);
```

### 2. Latitude & Temperature Correlation

Cities at higher latitudes have cooler average temperatures:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('weather_graph', $$
    MATCH (l:Location)-[:HAS_WEATHER]->(w:WeatherReading)
    RETURN l.name as city, l.latitude as lat,
           round(avg(w.high_temp_f)::numeric, 1) as avg_high,
           round(avg(w.low_temp_f)::numeric, 1) as avg_low
    ORDER BY l.latitude DESC
$$) as (city agtype, lat agtype, avg_high agtype, avg_low agtype);
```

### 3. Coastal Proximity & Temperature Moderation

Coastal cities have smaller temperature ranges (ocean moderating effect):

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('weather_graph', $$
    MATCH (l:Location)-[:HAS_WEATHER]->(w:WeatherReading)
    WITH l.name as city, l.distance_to_coast_km as coast_dist,
         avg(w.high_temp_f - w.low_temp_f) as avg_range
    RETURN city, coast_dist,
           round(avg_range::numeric, 1) as daily_temp_range,
           CASE WHEN coast_dist < 100 THEN 'coastal'
                WHEN coast_dist < 300 THEN 'near_coast'
                ELSE 'inland' END as region
    ORDER BY coast_dist
$$) as (city agtype, coast_dist agtype, daily_temp_range agtype, region agtype);
```

### 4. Rain Probability Given High Humidity

Conditional probability analysis:

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('weather_graph', $$
    MATCH (h:Humidity)-[:CONCURRENT_WITH]->(p:Precipitation)
    WHERE h.value_percent > 70
    WITH h.location as loc,
         count(CASE WHEN p.value_inches > 0 THEN 1 END) as rain_count,
         count(p) as total
    RETURN loc, rain_count, total,
           round((100.0 * rain_count / total)::numeric, 1) as rain_prob_pct
$$) as (location agtype, rain_count agtype, total agtype, rain_prob_pct agtype)
ORDER BY rain_prob_pct DESC;
```

### 5. Weather Similarity Between Cities

Compare weather between nearby cities (change `'Huntsville'` to any city in your graph):

```sql
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

SELECT * FROM cypher('weather_graph', $$
    MATCH (origin:Location {name: 'Huntsville'})-[:NEAR]->(nearby:Location),
          (origin)-[:HAS_WEATHER]->(w1:WeatherReading),
          (nearby)-[:HAS_WEATHER]->(w2:WeatherReading)
    WHERE w1.date = w2.date
    RETURN nearby.name, w1.date, w2.high_temp_f,
           abs(w2.high_temp_f - w1.high_temp_f) as temp_diff
$$) as (city agtype, date agtype, high_temp agtype, temp_diff agtype)
ORDER BY date, temp_diff
LIMIT 10;
```

