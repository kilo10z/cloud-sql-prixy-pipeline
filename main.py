import os
import psycopg2
import subprocess
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def execute_sql(request):
    # Environment Variables
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_name = os.getenv("DB_NAME")
    sql_query = os.getenv("SQL_QUERY")

    logger.info("Environment variables:")
    logger.info(f"INSTANCE_CONNECTION_NAME: {instance_connection_name}")
    logger.info(f"DB_USER: {db_user}")
    logger.info(f"DB_NAME: {db_name}")

    # Start Cloud SQL Proxy
    try:
        logger.info("Starting Cloud SQL Proxy...")
        proxy_command = [
            "/tmp/cloud_sql_proxy",
            f"-instances={instance_connection_name}=tcp:5432",
            "--auto-iam-authn"
        ]
        proxy_process = subprocess.Popen(proxy_command)
    except Exception as e:
        logger.error(f"Failed to start Cloud SQL Proxy: {e}")
        return {"status": "error", "message": f"Failed to start Cloud SQL Proxy: {e}"}, 500

    # Wait for Proxy to Start
    import time
    time.sleep(5)

    # Connect to Cloud SQL
    try:
        logger.info("Connecting to the database...")
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host="127.0.0.1",
            port="5432",
            sslmode="disable"
        )
        logger.info("Connected to the database.")
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return {"status": "error", "message": f"Failed to connect to the database: {e}"}, 500

    # Execute SQL Query
    try:
        logger.info("Executing SQL query...")
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("SQL query executed successfully.")
    except Exception as e:
        logger.error(f"Failed to execute SQL query: {e}")
        return {"status": "error", "message": f"Failed to execute SQL query: {e}"}, 500
    finally:
        conn.close()

    return {"status": "success", "message": "SQL query executed successfully."}, 200
