from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import os
import logging
import glob # Used to find the production files

# ---------- 1. SETUP: CONSTANTS AND CONFIGURATION ----------

# Sets the base directory to the dags/ folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Sets the data directory to dags/data/
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Defaults to the Postgres service from docker-compose.yaml; override with ETL_DB_URL
DB_URL = os.environ.get("ETL_DB_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")

# Standard file names for our staging process
# Raw files (must match your uploads in dags/data)
RAW_CRASHES = os.path.join(DATA_DIR, "Crash_Reporting.csv")
RAW_EV = os.path.join(DATA_DIR, "Electric_Vehicle_Population_Data.csv")
RAW_TRANSPORT = os.path.join(DATA_DIR, "share-with-convenient-access-to-public-transport.csv")
RAW_SALES = os.path.join(DATA_DIR, "car-sales.csv")
RAW_EEA = os.path.join(DATA_DIR, "eea.csv")
RAW_EPA = os.path.join(DATA_DIR, "epa.csv")
RAW_AIR_TRAFFIC = os.path.join(DATA_DIR, "Air_Traffic_Passenger_Statistics.csv")

# Staged files (intermediate)
STAGED_CRASHES = os.path.join(DATA_DIR, "crashes_staged.csv")
STAGED_EV = os.path.join(DATA_DIR, "ev_staged.csv")
STAGED_TRANSPORT = os.path.join(DATA_DIR, "transport_staged.csv")
STAGED_SALES = os.path.join(DATA_DIR, "sales_staged.csv")
STAGED_EEA = os.path.join(DATA_DIR, "eea_staged.csv")
STAGED_EPA = os.path.join(DATA_DIR, "epa_staged.csv")
STAGED_PROD = os.path.join(DATA_DIR, "vehicle_prod_staged.csv")
STAGED_AIR_TRAFFIC = os.path.join(DATA_DIR, "air_traffic_staged.csv")

# Transformed files (clean)
TRANSFORMED_CRASHES = os.path.join(DATA_DIR, "crashes_transformed.csv")
TRANSFORMED_EV = os.path.join(DATA_DIR, "ev_transformed.csv")
TRANSFORMED_TRANSPORT = os.path.join(DATA_DIR, "transport_transformed.csv")
TRANSFORMED_SALES = os.path.join(DATA_DIR, "sales_transformed.csv")
TRANSFORMED_EEA = os.path.join(DATA_DIR, "eea_transformed.csv")
TRANSFORMED_EPA = os.path.join(DATA_DIR, "epa_transformed.csv")
TRANSFORMED_PROD = os.path.join(DATA_DIR, "vehicle_prod_transformed.csv")
TRANSFORMED_AIR_TRAFFIC = os.path.join(DATA_DIR, "air_traffic_transformed.csv")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# ---------- 2. CREATE TABLES FUNCTION ----------

def create_tables():
    """Create all target tables in PostgreSQL if they don't exist."""
    engine = create_engine(DB_URL)
    
    # Note: Using TEXT for flexibility. You can optimize with VARCHAR(N) later.
    # Using NUMERIC for financial and coordinate data.
    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS crash_reporting (
        "Report Number" TEXT PRIMARY KEY,
        "Local Case Number" TEXT,
        "Agency Name" TEXT,
        "ACRS Report Type" TEXT,
        "Crash Date/Time" TIMESTAMP,
        "Route Type" TEXT,
        "Road Name" TEXT,
        "Cross-Street Name" TEXT,
        "Off-Road Description" TEXT,
        "Municipality" TEXT,
        "Related Non-Motorist" TEXT,
        "Collision Type" TEXT,
        "Weather" TEXT,
        "Surface Condition" TEXT,
        "Light" TEXT,
        "Traffic Control" TEXT,
        "Driver Substance Abuse" TEXT,
        "Non-Motorist Substance Abuse" TEXT,
        "Person ID" TEXT,
        "Driver At Fault" TEXT,
        "Injury Severity" TEXT,
        "Circumstance" TEXT,
        "Driver Distracted By" TEXT,
        "Drivers License State" TEXT,
        "Vehicle ID" TEXT,
        "Vehicle Damage Extent" TEXT,
        "Vehicle First Impact Location" TEXT,
        "Vehicle Body Type" TEXT,
        "Vehicle Movement" TEXT,
        "Vehicle Going Dir" TEXT,
        "Speed Limit" INT,
        "Driverless Vehicle" TEXT,
        "Parked Vehicle" TEXT,
        "Vehicle Year" INT,
        "Vehicle Make" TEXT,
        "Vehicle Model" TEXT,
        "Latitude" NUMERIC,
        "Longitude" NUMERIC,
        "Location" TEXT
    );

    CREATE TABLE IF NOT EXISTS electric_vehicles (
        "VIN (1-10)" TEXT PRIMARY KEY,
        "County" TEXT,
        "City" TEXT,
        "State" TEXT,
        "Postal Code" TEXT,
        "Model Year" INT,
        "Make" TEXT,
        "Model" TEXT,
        "Electric Vehicle Type" TEXT,
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility" TEXT,
        "Electric Range" INT,
        "Base MSRP" INT,
        "Legislative District" TEXT,
        "DOL Vehicle ID" TEXT,
        "Vehicle Location" TEXT,
        "Electric Utility" TEXT,
        "2020 Census Tract" TEXT
    );

    CREATE TABLE IF NOT EXISTS public_transport_access (
        "Entity" TEXT,
        "Code" TEXT,
        "Year" INT,
        "proportion_of_population_that_has_convenient_access_to_public_transport__pct" NUMERIC,
        PRIMARY KEY ("Code", "Year")
    );

    CREATE TABLE IF NOT EXISTS car_sales (
        "Entity" TEXT,
        "Code" TEXT,
        "Year" INT,
        "Electric cars sold" INT,
        "Non-electric car sales" INT,
        PRIMARY KEY ("Entity", "Year") -- Updated Primary key, Code has nulls
    );

    CREATE TABLE IF NOT EXISTS epa_facilities (
        "GHGRP Facility ID" INT PRIMARY KEY,
        "FACILITY NAME" TEXT,
        "GHGRP - City" TEXT,
        "GHGRP - State" TEXT,
        "ORIS CODE" INT,
        "GHGRP - Power Plant Sector" TEXT
    );

    CREATE TABLE IF NOT EXISTS vehicle_production (
        "Country" TEXT,
        "Vehicle Type" TEXT, -- e.g., 'Passenger Car', 'LCV', 'Truck', 'Bus'
        "YTD 2019" NUMERIC,
        "YTD 2021" NUMERIC,
        "YTD 2022" NUMERIC,
        "YTD 2023" NUMERIC,
        "YTD 2024" NUMERIC,
        PRIMARY KEY ("Country", "Vehicle Type")
    );
    CREATE TABLE IF NOT EXISTS air_traffic_passenger (
        activity_date TIMESTAMP,
        airline TEXT,
        geo_summary TEXT,
        geo_region TEXT,
        activity_type TEXT,
        passenger_count INT
    );
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_tables_sql))
        logging.info("All tables created successfully (or already exist).")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        raise

# ---------- 3. E-T-L FUNCTIONS ----------

# --- ETL Chain 1: Crash Reporting ---

def extract_crashes():
    """Extracts raw crash data and saves it to a staged CSV."""
    logging.info(f"Extracting {RAW_CRASHES}")
    df = pd.read_csv(RAW_CRASHES, low_memory=False)
    df.to_csv(STAGED_CRASHES, index=False)
    logging.info(f"Staged {len(df)} crash records to {STAGED_CRASHES}")

def transform_crashes():
    """Transforms staged crash data."""
    logging.info("Transforming staged crash data...")
    df = pd.read_csv(STAGED_CRASHES, low_memory=False)
    
    # --- [FIXED] Minimal transformation ---
    # Convert types for columns that we defined with specific types
    df['Crash Date/Time'] = pd.to_datetime(df['Crash Date/Time'], errors='coerce')
    df['Speed Limit'] = pd.to_numeric(df['Speed Limit'], errors='coerce')
    df['Vehicle Year'] = pd.to_numeric(df['Vehicle Year'], errors='coerce')
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

    # Drop rows where the primary key is missing
    df = df.dropna(subset=['Report Number'])
    # ------------------------------------
    
    df.to_csv(TRANSFORMED_CRASHES, index=False)
    logging.info(f"Saved {len(df)} transformed crash records to {TRANSFORMED_CRASHES}")

def load_crashes():
    """Loads transformed crash data into PostgreSQL, replacing existing data."""
    logging.info("Loading transformed crash data into database...")
    df = pd.read_csv(TRANSFORMED_CRASHES, low_memory=False)
    engine = create_engine(DB_URL)
    
    # Use 'replace' to ensure data is fresh.
    df.to_sql('crash_reporting', engine, if_exists='replace', index=False, chunksize=10000)
    logging.info(f"Loaded {len(df)} crash records into crash_reporting table.")

# --- ETL Chain 2: Electric Vehicles ---

def extract_ev():
    """Extracts raw EV data and saves it to a staged CSV."""
    logging.info(f"Extracting {RAW_EV}")
    df = pd.read_csv(RAW_EV, low_memory=False)
    df.to_csv(STAGED_EV, index=False)
    logging.info(f"Staged {len(df)} EV records to {STAGED_EV}")

def transform_ev():
    """Transforms staged EV data."""
    logging.info("Transforming staged EV data...")
    df = pd.read_csv(STAGED_EV, low_memory=False)
    
    # --- Minimal transformation ---
    df['Model Year'] = pd.to_numeric(df['Model Year'], errors='coerce')
    df['Electric Range'] = pd.to_numeric(df['Electric Range'], errors='coerce')
    df['Base MSRP'] = pd.to_numeric(df['Base MSRP'], errors='coerce')
    
    # Drop rows where the primary key is missing
    df = df.dropna(subset=['VIN (1-10)'])
    
    # --- [NOTE] ---
    # We are NOT dropping columns (like 'Make', 'Model' etc.) as requested
    # because the 'CREATE TABLE' statement requires them.
    # The 'transform_data' snippet provided was for a different schema.
    # ------------------------------------
    
    df.to_csv(TRANSFORMED_EV, index=False)
    logging.info(f"Saved {len(df)} transformed EV records to {TRANSFORMED_EV}")

def load_ev():
    """Loads transformed EV data into PostgreSQL, replacing existing data."""
    logging.info("Loading transformed EV data into database...")
    df = pd.read_csv(TRANSFORMED_EV, low_memory=False)
    engine = create_engine(DB_URL)
    df.to_sql('electric_vehicles', engine, if_exists='replace', index=False, chunksize=10000)
    logging.info(f"Loaded {len(df)} EV records into electric_vehicles table.")

# --- ETL Chain 3: Public Transport ---

def extract_public_transport():
    """Extracts raw public transport data and saves it to a staged CSV."""
    logging.info(f"Extracting {RAW_TRANSPORT}")
    df = pd.read_csv(RAW_TRANSPORT)
    df.to_csv(STAGED_TRANSPORT, index=False)
    logging.info(f"Staged {len(df)} transport records to {STAGED_TRANSPORT}")

def transform_public_transport():
    """Transforms staged public transport data."""
    logging.info("Transforming staged public transport data...")
    df = pd.read_csv(STAGED_TRANSPORT)
    
    # --- [FIXED] Minimal transformation ---
    # Column names are already clean
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    df['proportion_of_population_that_has_convenient_access_to_public_transport__pct'] = pd.to_numeric(df['proportion_of_population_that_has_convenient_access_to_public_transport__pct'], errors='coerce')
    df = df.dropna(subset=['Code', 'Year'])
    # ------------------------------------
    
    df.to_csv(TRANSFORMED_TRANSPORT, index=False)
    logging.info(f"Saved {len(df)} transformed transport records to {TRANSFORMED_TRANSPORT}")

def load_public_transport():
    """Loads transformed public transport data into PostgreSQL, replacing existing data."""
    logging.info("Loading transformed public transport data into database...")
    df = pd.read_csv(TRANSFORMED_TRANSPORT)
    engine = create_engine(DB_URL)
    df.to_sql('public_transport_access', engine, if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} transport records into public_transport_access table.")

# --- ETL Chain 4: Car Sales ---

def extract_car_sales():
    """Extracts raw car sales data and saves it to a staged CSV."""
    logging.info(f"Extracting {RAW_SALES}")
    df = pd.read_csv(RAW_SALES)
    df.to_csv(STAGED_SALES, index=False)
    logging.info(f"Staged {len(df)} sales records to {STAGED_SALES}")

def transform_car_sales():
    """Transforms staged car sales data."""
    logging.info("Transforming staged car sales data...")
    df = pd.read_csv(STAGED_SALES)
    
    # --- [UPDATED] Using your logic ---
    df = df.drop(columns=['Code'], errors='ignore') # As requested
    df = df.drop_duplicates()
    
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
    # Fill all other NaNs with 0 as requested
    df['Electric cars sold'] = pd.to_numeric(df['Electric cars sold'], errors='coerce').fillna(0)
    df['Non-electric car sales'] = pd.to_numeric(df['Non-electric car sales'], errors='coerce').fillna(0)
    
    df = df.dropna(subset=['Entity', 'Year'])
    # ------------------------------------
    
    df.to_csv(TRANSFORMED_SALES, index=False)
    logging.info(f"Saved {len(df)} transformed sales records to {TRANSFORMED_SALES}")

def load_car_sales():
    """Loads transformed car sales data into PostgreSQL, replacing existing data."""
    logging.info("Loading transformed car sales data into database...")
    df = pd.read_csv(TRANSFORMED_SALES)
    engine = create_engine(DB_URL)
    df.to_sql('car_sales', engine, if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} sales records into car_sales table.")

# --- ETL Chain 6: EPA ---

def extract_epa():
    """Extracts raw EPA data and saves it to a staged CSV."""
    logging.info(f"Extracting {RAW_EPA}")
    df = pd.read_csv(RAW_EPA, low_memory=False)
    df.to_csv(STAGED_EPA, index=False)
    logging.info(f"Staged {len(df)} EPA records to {STAGED_EPA}")

def transform_epa():
    """Transforms staged EPA data."""
    logging.info("Transforming staged EPA data...")
    df = pd.read_csv(STAGED_EPA, low_memory=False)
    
    # --- [FIXED] This transformation is REQUIRED ---
    # Select only columns we defined in the table
    cols_to_keep = [
        "GHGRP Facility ID", "FACILITY NAME", "GHGRP - City",
        "GHGRP - State", "ORIS CODE", "GHGRP - Power Plant Sector"
    ]
    
    # Find columns that actually exist
    existing_cols = [col for col in cols_to_keep if col in df.columns]
    df = df[existing_cols]
    
    # Clean types
    df['GHGRP Facility ID'] = pd.to_numeric(df['GHGRP Facility ID'], errors='coerce')
    df['ORIS CODE'] = pd.to_numeric(df['ORIS CODE'], errors='coerce')
    
    df = df.dropna(subset=["GHGRP Facility ID"])
    df['GHGRP Facility ID'] = df['GHGRP Facility ID'].astype(int)
    # ------------------------------------
    
    df.to_csv(TRANSFORMED_EPA, index=False)
    logging.info(f"Saved {len(df)} transformed EPA records to {TRANSFORMED_EPA}")

def load_epa():
    """Loads transformed EPA data into PostgreSQL, replacing existing data."""
    logging.info("Loading transformed EPA data into database...")
    df = pd.read_csv(TRANSFORMED_EPA, low_memory=False)
    engine = create_engine(DB_URL)
    
    # Select only the columns we need, in case extra ones are still present
    cols_to_load = [
        "GHGRP Facility ID", "FACILITY NAME", "GHGRP - City",
        "GHGRP - State", "ORIS CODE", "GHGRP - Power Plant Sector"
    ]
    existing_cols = [col for col in cols_to_load if col in df.columns]
    
    df[existing_cols].to_sql('epa_facilities', engine, if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} EPA records into epa_facilities table.")

def extract_air_traffic():
    """Extrait les données brutes sur le trafic aérien et les sauvegarde dans un CSV de transit."""
    logging.info(f"Extraction de {RAW_AIR_TRAFFIC}")
    try:
        # Note : votre script d'origine utilisait 'Air_Traffic_Passenger_Statistics.csv'
        df = pd.read_csv(RAW_AIR_TRAFFIC, low_memory=False)
        df.to_csv(STAGED_AIR_TRAFFIC, index=False)
        logging.info(f"Mise en transit de {len(df)} lignes de trafic aérien vers {STAGED_AIR_TRAFFIC}")
    except FileNotFoundError:
        logging.error(f"ERREUR : Fichier {RAW_AIR_TRAFFIC} non trouvé.")
        raise
    except Exception as e:
        logging.error(f"ERREUR lors de l'extraction du trafic aérien : {e}")
        raise

def transform_air_traffic():
    """Transforme les données de trafic aérien en transit (logique de votre script)."""
    logging.info("Transformation des données de trafic aérien...")
    df = pd.read_csv(STAGED_AIR_TRAFFIC, low_memory=False)
    
    df_transformed = df.copy()
    
    # T.1: Sélectionner uniquement les colonnes pertinentes
    colonnes_pertinentes = [
        'Activity Period Start Date',
        'Published Airline',
        'GEO Summary',
        'GEO Region',
        'Activity Type Code',
        'Passenger Count'
    ]
    
    cols_existantes = [col for col in colonnes_pertinentes if col in df_transformed.columns]
    df_transformed = df_transformed[cols_existantes]
    
    # T.2: Renommer les colonnes
    nouveaux_noms = {
        'Activity Period Start Date': 'activity_date',
        'Published Airline': 'airline',
        'GEO Summary': 'geo_summary',
        'GEO Region': 'geo_region',
        'Activity Type Code': 'activity_type',
        'Passenger Count': 'passenger_count'
    }
    df_transformed = df_transformed.rename(columns=nouveaux_noms)
    
    # T.3: Convertir les types de données
    if 'activity_date' in df_transformed.columns:
        df_transformed['activity_date'] = pd.to_datetime(df_transformed['activity_date'])
    if 'passenger_count' in df_transformed.columns:
        df_transformed['passenger_count'] = pd.to_numeric(df_transformed['passenger_count'], errors='coerce').fillna(0).astype(int)
    
    # T.4: Nettoyage (supprimer les lignes où la date ou la compagnie est nulle)
    cols_nettoyage = [col for col in ['activity_date', 'airline'] if col in df_transformed.columns]
    if cols_nettoyage:
        df_transformed = df_transformed.dropna(subset=cols_nettoyage)
    
    df_transformed.to_csv(TRANSFORMED_AIR_TRAFFIC, index=False)
    logging.info(f"Sauvegarde de {len(df_transformed)} lignes de trafic aérien transformées dans {TRANSFORMED_AIR_TRAFFIC}")

def load_air_traffic():
    """Charge les données de trafic aérien transformées dans PostgreSQL."""
    logging.info("Chargement des données de trafic aérien transformées dans la base de données...")
    df = pd.read_csv(TRANSFORMED_AIR_TRAFFIC, low_memory=False)
    engine = create_engine(DB_URL)
    
    # Votre script utilise 'replace', ce qui est parfait pour ce modèle
    df.to_sql('air_traffic_passenger', engine, if_exists='replace', index=False, chunksize=10000)
    logging.info(f"Chargement de {len(df)} lignes de trafic aérien dans la table 'air_traffic_passenger'.")
# --- ETL Chain 7: GTFS Transport Data ---

GTFS_FILES = [
    ("agency.csv", "agency", ","),
    ("calendar_dates.csv", "calendar_dates", ","),
    ("fare_attributes.csv", "fare_attributes", ","),
    ("fare_rules.csv", "fare_rules", ","),
    ("fiches-horaires-et-plans.csv", "fiches_horaires_et_plans", ";"),
    ("routes.csv", "routes", ","),
    ("stops.csv", "stops", ","),
    ("trips.csv", "trips", ","),
]

def extract_gtfs():
    """Extract all GTFS CSV files and save them to staging."""
    logging.info("Extracting GTFS CSVs...")
    for filename, table_name, delim in GTFS_FILES:
        src = os.path.join(DATA_DIR, filename)
        dst = os.path.join(DATA_DIR, f"{table_name}_staged.csv")
        if os.path.exists(src):
            df = pd.read_csv(src, delimiter=delim, low_memory=False)
            df.to_csv(dst, index=False)
            logging.info(f"Staged {len(df)} rows from {filename} to {dst}")
        else:
            logging.warning(f"GTFS file missing: {filename}")

def transform_gtfs():
    """Apply minimal transformations to GTFS data."""
    logging.info("Transforming GTFS CSVs...")
    for _, table_name, _ in GTFS_FILES:
        staged = os.path.join(DATA_DIR, f"{table_name}_staged.csv")
        transformed = os.path.join(DATA_DIR, f"{table_name}_transformed.csv")
        if os.path.exists(staged):
            df = pd.read_csv(staged, low_memory=False)
            df = df.drop_duplicates()
            df.to_csv(transformed, index=False)
            logging.info(f"Transformed {len(df)} rows for {table_name}")
        else:
            logging.warning(f"Missing staged GTFS file: {table_name}")

def load_gtfs():
    """Load all GTFS transformed data into PostgreSQL."""
    logging.info("Loading GTFS data into PostgreSQL...")
    engine = create_engine(DB_URL)
    for _, table_name, _ in GTFS_FILES:
        transformed = os.path.join(DATA_DIR, f"{table_name}_transformed.csv")
        if os.path.exists(transformed):
            df = pd.read_csv(transformed, low_memory=False)
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            logging.info(f"Loaded {len(df)} rows into {table_name}")
        else:
            logging.warning(f"Missing transformed GTFS file: {table_name}")


# ---------- 4. DATA QUALITY CHECK FUNCTION ----------

def data_quality_check():
    """Runs simple row count checks on all loaded tables."""
    logging.info("Running data quality checks...")
    engine = create_engine(DB_URL)
    tables_to_check = [
        "crash_reporting",
        "electric_vehicles",
        "public_transport_access",
        "car_sales",
        "epa_facilities",
        "air_traffic_passenger"
    ]
    
    with engine.connect() as conn:
        for table in tables_to_check:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
                row_count = result[0]
                if row_count > 0:
                    logging.info(f"✅ Data quality check PASSED for {table}: {row_count} rows found.")
                else:
                    logging.warning(f"⚠️ Data quality check FAILED for {table}: 0 rows found.")
            except Exception as e:
                logging.error(f"❌ Data quality check ERROR for {table}: {e}")
                raise


# ---------- 5. DAG DEFINITION ----------

with DAG(
    dag_id='transport_sector_etl_dag',
    default_args=default_args,
    description='Full ETL pipeline for the transport sector.',
    schedule=None, # You can change this to '@daily', '@weekly', etc.
    catchup=False,
    tags=['vehicles', 'etl', 'project', 'staging']
) as dag:

    start = EmptyOperator(task_id='start_pipeline')

    create_tables_task = PythonOperator(
        task_id='create_database_tables',
        python_callable=create_tables
    )

    # --- Define all EXTRACT tasks ---
    extract_crashes_task = PythonOperator(task_id='extract_crashes', python_callable=extract_crashes)
    extract_ev_task = PythonOperator(task_id='extract_electric_vehicles', python_callable=extract_ev)
    extract_transport_task = PythonOperator(task_id='extract_public_transport', python_callable=extract_public_transport)
    extract_sales_task = PythonOperator(task_id='extract_car_sales', python_callable=extract_car_sales)
    extract_epa_task = PythonOperator(task_id='extract_epa', python_callable=extract_epa)
    extract_air_traffic_task=PythonOperator(task_id='extract_air_traffic', python_callable=extract_air_traffic)
    extract_gtfs_task = PythonOperator(task_id='extract_gtfs', python_callable=extract_gtfs)

    # --- Define all TRANSFORM tasks ---
    transform_crashes_task = PythonOperator(task_id='transform_crashes', python_callable=transform_crashes)
    transform_ev_task = PythonOperator(task_id='transform_electric_vehicles', python_callable=transform_ev)
    transform_transport_task = PythonOperator(task_id='transform_public_transport', python_callable=transform_public_transport)
    transform_sales_task = PythonOperator(task_id='transform_car_sales', python_callable=transform_car_sales)
    transform_epa_task = PythonOperator(task_id='transform_epa', python_callable=transform_epa)
    transform_air_traffic_task = PythonOperator(task_id='transform_air_traffic', python_callable=transform_air_traffic)
    transform_gtfs_task = PythonOperator(task_id='transform_gtfs', python_callable=transform_gtfs)
    
    # --- Define all LOAD tasks ---
    load_crashes_task = PythonOperator(task_id='load_crashes', python_callable=load_crashes)
    load_ev_task = PythonOperator(task_id='load_electric_vehicles', python_callable=load_ev)
    load_transport_task = PythonOperator(task_id='load_public_transport', python_callable=load_public_transport)
    load_sales_task = PythonOperator(task_id='load_car_sales', python_callable=load_car_sales)
    load_epa_task = PythonOperator(task_id='load_epa', python_callable=load_epa)
    load_air_traffic_task = PythonOperator(task_id='load_air_traffic', python_callable=load_air_traffic)
    load_gtfs_task = PythonOperator(task_id='load_gtfs', python_callable=load_gtfs)

    # --- Define final Data Quality Check task ---
    data_quality_check_task = PythonOperator(
        task_id='run_data_quality_checks',
        python_callable=data_quality_check
    )

    end = EmptyOperator(task_id='end_pipeline')

    # ---------- 6. DAG WORKFLOW ORCHESTRATION ----------

    start >> create_tables_task

  
    extract_tasks = [
        extract_crashes_task,
        extract_ev_task,
        extract_transport_task,
        extract_sales_task,
        extract_epa_task,
        extract_air_traffic_task,
        extract_gtfs_task
    ]
    create_tables_task >> extract_tasks

    (extract_crashes_task >> transform_crashes_task >> load_crashes_task)
    (extract_ev_task >> transform_ev_task >> load_ev_task)
    (extract_transport_task >> transform_transport_task >> load_transport_task)
    (extract_sales_task >> transform_sales_task >> load_sales_task)
    (extract_epa_task >> transform_epa_task >> load_epa_task)
    (extract_air_traffic_task >> transform_air_traffic_task >> load_air_traffic_task)
    (extract_gtfs_task >> transform_gtfs_task >> load_gtfs_task)

    load_tasks = [
        load_crashes_task,
        load_ev_task,
        load_transport_task,
        load_sales_task,
        load_epa_task,
        load_air_traffic_task,
        load_gtfs_task
    ]
    
    # 5. Run the DQ check only after ALL load tasks are finished
    load_tasks >> data_quality_check_task
    
    # 6. End the pipeline
    data_quality_check_task >> end