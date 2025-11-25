.PHONY: up down restart weather cities backup restore package logs

# Load environment variables from .env file
include .env.example
-include .env
export

# Starts all services (database, API, Airflow, pgAdmin)
up:
	@mkdir -p logs backups dags
	@chmod 777 logs backups 2>/dev/null || true
	@test -f .env || cp .env.example .env
	@echo "Starting all services..."
	docker compose up -d
	@echo ""
	@echo "✓ All services started"
	@echo ""
	@echo "Web Interfaces:"
	@echo "  FastAPI: http://localhost:8000/docs"
	@echo "  Airflow: http://localhost:8080 (admin/admin)"
	@echo "  pgAdmin: http://localhost:5050 (admin@example.com/admin)"
	@echo ""
	@echo "Daily backups run at 3:00 AM."
	@echo "Apache AGE graph extension initializes automatically."

# Composes down service stack and removes unused images, volumes, and containers.
down:
	docker compose --profile pypi down --rmi all --volumes --remove-orphans

restart: down up

# $(MAKECMDGOALS) = all args passed to make. $(filter-out $@,...) strips target name.
# $(if condition,then,else) falls back to default when no location arg provided.
weather:
	@docker exec -it api python -m app.ingest "$(if $(filter-out $@,$(MAKECMDGOALS)),$(filter-out $@,$(MAKECMDGOALS)),Huntsville, AL)"

# Catch-all prevents make from erroring on unrecognized location argument
%:
	@:

# Creates graph nodes for default Southeast US cities
cities:
	@docker exec -it api python -m app.create_node_relationships

# Creates a backup of weather data to Parquet
backup:
	docker exec -it api python -m app.backup_data

# Restores Postgres database from backup
restore:
	docker exec -it api python -m app.restore_from_backup

# Builds PyPI package and publishes it to local server
package:
	python -m build && cp dist/* local-pypi/packages/
	docker compose --profile pypi up -d pypi
	@echo ""
	@echo "Package deployed to local PyPI server: http://localhost:8081"

# View container logs
logs:
	docker compose logs -f
