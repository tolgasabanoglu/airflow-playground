"""
Garmin Daily Health Metrics Pipeline

This DAG orchestrates the daily fetching of Garmin health data and loads it into BigQuery:
1. Fetch yesterday's daily metrics (steps, sleep, stress, HR, body battery)
2. Fetch new activities (running, cycling, etc.)
3. Load JSON data to BigQuery raw table
4. Deploy transformation views (7 views)
5. Deploy dashboard views (5 views)

Schedule: Daily at 2 AM
Data fetched: Previous day's metrics
"""

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging

log = logging.getLogger(__name__)

# ============ CONFIGURATION ============
SPATIOTEMPORAL_PATH = '/opt/airflow/spatiotemporal'  # Docker mount point
RAW_DATA_PATH = os.path.join(SPATIOTEMPORAL_PATH, 'data', 'raw')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}


# ============ TASK 1: FETCH GARMIN DAILY METRICS ============
def fetch_garmin_daily_metrics(**context):
    """Fetch yesterday's Garmin metrics (steps, sleep, stress, HR, body battery)"""
    execution_date = context['execution_date']
    # Always fetch yesterday's data (completed metrics)
    target_date = (execution_date - timedelta(days=1)).date()
    date_str = target_date.isoformat()

    log.info(f"Fetching Garmin metrics for {date_str}")

    # Add spatiotemporal to Python path
    sys.path.insert(0, os.path.join(SPATIOTEMPORAL_PATH, 'garmin'))

    # Import dependencies
    import json
    import time
    from garminconnect import Garmin

    # Get credentials from environment (set by docker-compose)
    username = os.getenv('GARMIN_USERNAME')
    password = os.getenv('GARMIN_PASSWORD')

    if not username or not password:
        raise ValueError("Missing Garmin credentials in environment")

    # Login to Garmin
    log.info("Logging into Garmin Connect...")
    client = Garmin(username, password)
    client.login()
    log.info("Successfully logged in")

    # Ensure raw data directory exists
    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    # Define metrics to fetch
    fetchers = {
        'steps': client.get_steps_data,
        'sleep': client.get_sleep_data,
        'stress': client.get_stress_data,
        'body_battery': client.get_body_battery,
        'heart_rate': client.get_heart_rates,
    }

    # Fetch each metric
    metrics_fetched = []
    metrics_skipped = []
    metrics_failed = []

    for metric_name, fetch_func in fetchers.items():
        file_path = os.path.join(RAW_DATA_PATH, f"{metric_name}_{date_str}.json")

        if os.path.exists(file_path):
            log.info(f"Skipping {metric_name} (already exists)")
            metrics_skipped.append(metric_name)
            continue

        try:
            log.info(f"Fetching {metric_name}...")
            data = fetch_func(date_str)

            # Validate data (check if meaningful data exists)
            is_valid = False
            if isinstance(data, list):
                is_valid = len(data) > 0
            elif isinstance(data, dict):
                is_valid = any(v not in (None, 0, [], {}, "", "null") for v in data.values())

            if is_valid:
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2)
                log.info(f"Saved {metric_name}")
                metrics_fetched.append(metric_name)
            else:
                log.warning(f"No valid {metric_name} data for {date_str}")

        except Exception as e:
            log.error(f"Error fetching {metric_name}: {e}")
            metrics_failed.append(metric_name)
            # Don't fail entire task if one metric fails

        time.sleep(1)  # Rate limiting

    log.info(f"Summary:")
    log.info(f"  - Fetched: {len(metrics_fetched)} ({', '.join(metrics_fetched) if metrics_fetched else 'none'})")
    log.info(f"  - Skipped: {len(metrics_skipped)} ({', '.join(metrics_skipped) if metrics_skipped else 'none'})")
    log.info(f"  - Failed: {len(metrics_failed)} ({', '.join(metrics_failed) if metrics_failed else 'none'})")

    return {
        'date': date_str,
        'metrics_fetched': metrics_fetched,
        'metrics_skipped': metrics_skipped,
        'metrics_failed': metrics_failed
    }


# ============ TASK 2: FETCH ACTIVITIES ============
def fetch_garmin_activities(**context):
    """Fetch activity details (incremental - only new activities)"""
    execution_date = context['execution_date']
    # Fetch activities up to yesterday (to match metrics date)
    target_date = (execution_date - timedelta(days=1)).date()

    log.info(f"Fetching Garmin activities up to {target_date}")

    sys.path.insert(0, os.path.join(SPATIOTEMPORAL_PATH, 'garmin'))

    import json
    import time
    from garminconnect import Garmin

    username = os.getenv('GARMIN_USERNAME')
    password = os.getenv('GARMIN_PASSWORD')

    if not username or not password:
        raise ValueError("Missing Garmin credentials in environment")

    log.info("Logging into Garmin Connect...")
    client = Garmin(username, password)
    client.login()
    log.info("Successfully logged in")

    activities_dir = os.path.join(RAW_DATA_PATH, 'activities')
    os.makedirs(activities_dir, exist_ok=True)

    # Fetch activities list (last 100)
    log.info("Fetching activities list...")
    try:
        activities = client.get_activities(0, 100)

        # Save activities list
        list_file = os.path.join(activities_dir, 'activities_list.json')
        with open(list_file, 'w') as f:
            json.dump(activities, f, indent=2)
        log.info(f"Saved activities list ({len(activities)} activities)")

        new_activities = 0
        existing_activities = 0

        for activity in activities:
            activity_id = activity.get('activityId')
            if not activity_id:
                continue

            detail_file = f"activity_{activity_id}_detail.json"
            detail_path = os.path.join(activities_dir, detail_file)

            if os.path.exists(detail_path):
                existing_activities += 1
                continue  # Skip existing

            # Fetch detail
            log.info(f"  Fetching activity {activity_id}...")
            try:
                details = client.get_activity(activity_id)
                if details:
                    with open(detail_path, 'w') as f:
                        json.dump(details, f, indent=2)
                    new_activities += 1
                    log.info(f"  Saved activity {activity_id}")
            except Exception as e:
                log.warning(f"  Failed to fetch activity {activity_id}: {e}")

            time.sleep(1)  # Rate limiting

        log.info(f"Summary:")
        log.info(f"  - New activities fetched: {new_activities}")
        log.info(f"  - Existing activities (skipped): {existing_activities}")

        return {
            'total_activities': len(activities),
            'new_activities': new_activities,
            'existing_activities': existing_activities
        }

    except Exception as e:
        log.error(f"Error fetching activities: {e}")
        raise


# ============ TASK 3: LOAD TO BIGQUERY ============
def load_to_bigquery_incremental(**context):
    """Load raw JSON files to BigQuery (incremental append)"""
    log.info("Loading data to BigQuery...")

    sys.path.insert(0, os.path.join(SPATIOTEMPORAL_PATH, 'garmin'))

    import json
    from google.cloud import bigquery

    # GOOGLE_APPLICATION_CREDENTIALS already set by environment
    client = bigquery.Client()

    dataset_id = f"{client.project}.garmin_data"
    table_id = f"{dataset_id}.garmin_raw_data"

    # Ensure dataset exists
    try:
        dataset = client.get_dataset(dataset_id)
        log.info(f"Dataset exists: {dataset_id}")
    except Exception:
        log.info(f"Creating dataset: {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)

    # Ensure table exists
    try:
        table = client.get_table(table_id)
        log.info(f"Table exists: {table_id}")
    except Exception:
        log.info(f"Creating table: {table_id}")
        schema = [
            bigquery.SchemaField("filename", "STRING"),
            bigquery.SchemaField("raw_json", "STRING")
        ]
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)

    # Get list of already loaded filenames to avoid duplicates
    log.info("Checking for already loaded files...")
    query = f"""
        SELECT DISTINCT filename
        FROM `{table_id}`
    """
    try:
        existing_files = {row.filename for row in client.query(query).result()}
        log.info(f"  Found {len(existing_files)} existing files in BigQuery")
    except Exception as e:
        log.warning(f"  Could not check existing files: {e}")
        existing_files = set()

    # Prepare rows (only files not already in BigQuery)
    files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith('.json')]
    rows = []
    skipped = []

    for file in files:
        if file in existing_files:
            skipped.append(file)
            continue

        path = os.path.join(RAW_DATA_PATH, file)
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                rows.append({"filename": file, "raw_json": json.dumps(data)})
        except Exception as e:
            log.warning(f"Failed to read {file}: {e}")

    if not rows:
        log.info("No new files to upload")
        return {'rows_uploaded': 0, 'files_skipped': len(skipped)}

    # Upload in chunks
    chunk_size = 200
    total_uploaded = 0

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        total_chunks = (len(rows) + chunk_size - 1) // chunk_size

        log.info(f"Uploading chunk {chunk_num}/{total_chunks} ({len(chunk)} rows)...")

        try:
            errors = client.insert_rows_json(table_id, chunk)
            if errors:
                log.error(f"Errors in chunk {chunk_num}: {errors}")
            else:
                log.info(f"  Chunk {chunk_num} uploaded successfully")
                total_uploaded += len(chunk)
        except Exception as e:
            log.error(f"Failed to upload chunk {chunk_num}: {e}")

    log.info(f"Summary:")
    log.info(f"  - Rows uploaded: {total_uploaded}")
    log.info(f"  - Files skipped (already loaded): {len(skipped)}")

    return {
        'rows_uploaded': total_uploaded,
        'files_skipped': len(skipped),
        'total_files': len(files)
    }


# ============ TASK 4: DEPLOY TRANSFORMATION VIEWS ============
def deploy_transformation_views(**context):
    """Deploy BigQuery transformation views from SQL file"""
    log.info("Deploying transformation views...")

    from google.cloud import bigquery

    client = bigquery.Client()
    sql_file = os.path.join(SPATIOTEMPORAL_PATH, 'garmin', 'sql', 'views.sql')

    log.info(f"Reading SQL file: {sql_file}")
    with open(sql_file, 'r') as f:
        sql_content = f.read()

    # Split into individual CREATE VIEW statements
    statements = []
    current = []

    for line in sql_content.split('\n'):
        if line.strip().startswith('CREATE OR REPLACE VIEW') and current:
            statements.append('\n'.join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        statements.append('\n'.join(current))

    # Filter to only statements that create views
    statements = [s for s in statements if 'CREATE OR REPLACE VIEW' in s]

    log.info(f"Deploying {len(statements)} views...")

    deployed = []
    failed = []

    for i, sql in enumerate(statements, 1):
        # Extract view name from SQL
        try:
            view_name = sql.split('`')[1] if '`' in sql else f"view_{i}"
        except:
            view_name = f"view_{i}"

        log.info(f"  [{i}/{len(statements)}] Deploying {view_name}...")

        try:
            job = client.query(sql)
            job.result()  # Wait for completion
            log.info(f"    Success")
            deployed.append(view_name)
        except Exception as e:
            log.error(f"    Error: {e}")
            failed.append(view_name)
            raise  # Fail the task if any view fails

    log.info(f"Summary:")
    log.info(f"  - Views deployed: {len(deployed)}")
    log.info(f"  - Views failed: {len(failed)}")

    return {
        'views_deployed': len(deployed),
        'views_failed': len(failed),
        'deployed_views': deployed
    }


# ============ TASK 5: DEPLOY DASHBOARD VIEWS ============
def deploy_dashboard_views(**context):
    """Deploy dashboard-specific BigQuery views"""
    log.info("Deploying dashboard views...")

    from google.cloud import bigquery

    client = bigquery.Client()
    sql_file = os.path.join(SPATIOTEMPORAL_PATH, 'dashboard', 'looker_views.sql')

    log.info(f"Reading SQL file: {sql_file}")
    with open(sql_file, 'r') as f:
        sql_content = f.read()

    # Split into individual CREATE VIEW statements
    statements = []
    current = []

    for line in sql_content.split('\n'):
        if line.strip().startswith('CREATE OR REPLACE VIEW') and current:
            statements.append('\n'.join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        statements.append('\n'.join(current))

    statements = [s for s in statements if 'CREATE OR REPLACE VIEW' in s]

    log.info(f"Deploying {len(statements)} dashboard views...")

    deployed = []
    failed = []

    for i, sql in enumerate(statements, 1):
        try:
            view_name = sql.split('`')[1] if '`' in sql else f"dashboard_view_{i}"
        except:
            view_name = f"dashboard_view_{i}"

        log.info(f"  [{i}/{len(statements)}] Deploying {view_name}...")

        try:
            job = client.query(sql)
            job.result()
            log.info(f"    Success")
            deployed.append(view_name)
        except Exception as e:
            log.error(f"    Error: {e}")
            failed.append(view_name)
            raise

    log.info(f"Summary:")
    log.info(f"  - Dashboard views deployed: {len(deployed)}")
    log.info(f"  - Dashboard views failed: {len(failed)}")

    return {
        'dashboard_views_deployed': len(deployed),
        'dashboard_views_failed': len(failed),
        'deployed_views': deployed
    }


# ============ DAG DEFINITION ============
with DAG(
    dag_id='garmin_daily_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,  # Don't backfill historical runs
    tags=['garmin', 'health', 'bigquery', 'daily', 'spatiotemporal'],
    description='Daily Garmin health metrics pipeline: fetch → load → transform',
    doc_md=__doc__,
) as dag:

    # Task 1: Fetch daily metrics
    task_fetch_metrics = PythonOperator(
        task_id='fetch_garmin_metrics',
        python_callable=fetch_garmin_daily_metrics,
        retries=5,  # Garmin API can be flaky
        retry_delay=timedelta(minutes=3),
    )

    # Task 2: Fetch activities (parallel with metrics)
    task_fetch_activities = PythonOperator(
        task_id='fetch_activities',
        python_callable=fetch_garmin_activities,
        retries=5,
        retry_delay=timedelta(minutes=3),
    )

    # Task 3: Load to BigQuery
    task_load_bq = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery_incremental,
        retries=2,
    )

    # Task 4: Deploy transformation views
    task_deploy_views = PythonOperator(
        task_id='deploy_transformation_views',
        python_callable=deploy_transformation_views,
        retries=1,
    )

    # Task 5: Deploy dashboard views
    task_deploy_dashboard = PythonOperator(
        task_id='deploy_dashboard_views',
        python_callable=deploy_dashboard_views,
        retries=1,
    )

    # Define dependencies: parallel fetch, then sequential transform
    [task_fetch_metrics, task_fetch_activities] >> task_load_bq >> task_deploy_views >> task_deploy_dashboard
