import os
import subprocess
import psycopg2
import logging
import time

def execute_sql(request):
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    # Fetch environment variables
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER")
    query = os.getenv("SQL_QUERY")

    try:
        # Log the start of the process
        logger.info("Starting Cloud SQL Proxy...")
        proxy_command = [
            "/tmp/cloud_sql_proxy",
            f"-instances={instance_connection_name}=tcp:5432",
            "--auto-iam-authn"
        ]

        # Start the Cloud SQL Proxy
        proxy_process = subprocess.Popen(proxy_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(5)  # Give the proxy time to start
        logger.info("Cloud SQL Proxy started successfully.")

        # Log database connection
        logger.info("Connecting to the database...")
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host="127.0.0.1",
            port=5432
        )
        cur = conn.cursor()

        # Log SQL query execution
        logger.info("Executing SQL query...")
        cur.execute(query)
        conn.commit()
        logger.info("SQL query executed successfully.")

        # Close the connection
        cur.close()
        conn.close()

        # Stop the Cloud SQL Proxy
        proxy_process.terminate()
        logger.info("Cloud SQL Proxy terminated.")
        return {"status": "success", "message": "SQL query executed successfully."}

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {"status": "error", "message": str(e)}
