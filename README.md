# Transport Sector BI — Airflow ETL

Apache Airflow 3 pipeline that loads transport-sector datasets into PostgreSQL
for BI reporting: crash reports, EV population, public-transport access, car sales,
EEA/EPA emissions, vehicle production, air traffic and GTFS (agencies, routes, stops,
trips, fares, calendar).

## Layout

```
dags/vehicle_environment_etl_dag.py   # DAG `transport_sector_etl_dag` (extract → transform → load)
dags/data/                            # Raw source files (CSV via Git LFS, XLSX)
config/airflow.cfg
docker-compose.yaml                   # Airflow (CeleryExecutor) + Postgres + Redis
```

Intermediate `*_staged.csv` / `*_transformed.csv` files are written to `dags/data/`
at run time and are not committed.

## Run

```bash
git lfs pull                 # fetch the CSV sources
cp .env.example .env         # then fill AIRFLOW_FERNET_KEY and AIRFLOW_JWT_SECRET
docker compose up airflow-init
docker compose up -d
```

Open http://localhost:8089 (default login `airflow` / `airflow`), then trigger
`transport_sector_etl_dag` manually (it has no schedule).

Set `ETL_DB_URL` to load into a database other than the compose Postgres.
