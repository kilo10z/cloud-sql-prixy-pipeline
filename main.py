import os
import psycopg2
import subprocess
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def execute_sql(request):
    # Environment Variables
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_user = os.getenv("DB_USER")
    db_name = os.getenv("DB_NAME")
    sql_query = os.getenv("SQL_QUERY")
    proxy_version = "2.12.0"

    # Check if required environment variables are set
    if not all([instance_connection_name, db_user, db_name, sql_query]):
        logger.error("Missing one or more required environment variables.")
        return {
            "status": "error",
            "message": "Missing one or more required environment variables. Ensure INSTANCE_CONNECTION_NAME, DB_USER, DB_NAME, and SQL_QUERY are set."
        }, 500

    # Define the path for the Cloud SQL Proxy binary
    proxy_path = f"/tmp/cloud_sql_proxy_{proxy_version}"

    # Download Cloud SQL Proxy if not already present
    if not os.path.exists(proxy_path):
        try:
            logger.info(f"Downloading Cloud SQL Proxy version {proxy_version}...")
            proxy_download_command = [
                "wget",
                f"https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v{proxy_version}/cloud-sql-proxy.linux.amd64",
                "-O", proxy_path
            ]
            subprocess.check_call(proxy_download_command)
            os.chmod(proxy_path, 0o755)  # Make the binary executable
            logger.info("Cloud SQL Proxy downloaded and made executable.")
        except Exception as e:
            logger.error(f"Failed to download Cloud SQL Proxy: {e}")
            return {
                "status": "error",
                "message": f"Failed to download Cloud SQL Proxy: {e}"
            }, 500

    # Start Cloud SQL Proxy
    try:
        logger.info("Starting Cloud SQL Proxy...")
        proxy_command = [
            proxy_path,
            f"--instances={instance_connection_name}=tcp:5432",
            "--enable_iam_login"
        ]
        proxy_process = subprocess.Popen(proxy_command, stderr=subprocess.PIPE, stdout=subprocess.PIPE)

        # Wait for the proxy to initialize
        time.sleep(15)
        stdout, stderr = proxy_process.communicate(timeout=20)

        # Check if the proxy started successfully
        if proxy_process.returncode != 0 or stderr:
            logger.error(f"Cloud SQL Proxy failed to start. Stdout: {stdout.decode()} Stderr: {stderr.decode()}")
            return {
                "status": "error",
                "message": f"Cloud SQL Proxy failed to start. Details: {stderr.decode()}"
            }, 500
        logger.info("Cloud SQL Proxy started successfully.")
    except subprocess.TimeoutExpired:
        logger.error("Cloud SQL Proxy failed to start within the timeout period.")
        return {
            "status": "error",
            "message": "Cloud SQL Proxy failed to start within the timeout period."
        }, 500
    except Exception as e:
        logger.error(f"Failed to start Cloud SQL Proxy: {e}")
        return {
            "status": "error",
            "message": f"Failed to start Cloud SQL Proxy: {e}"
        }, 500

    # Connect to Cloud SQL using IAM authentication
    try:
        logger.info("Connecting to the database...")
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host="127.0.0.1",
            port="5432",
            sslmode="disable",
            enable_iam_auth=True  # Enable IAM authentication
        )
        logger.info("Connected to the database.")
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        return {
            "status": "error",
            "message": f"Failed to connect to the database: {e}"
        }, 500

    # Execute SQL Query
    try:
        logger.info("Executing SQL query...")
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            logger.info("SQL query executed successfully.")
    except Exception as e:
        logger.error(f"Failed to execute SQL query: {e}")
        return {
            "status": "error",
            "message": f"Failed to execute SQL query: {e}"
        }, 500
    finally:
        conn.close()

    return {
        "status": "success",
        "message": "SQL query executed successfully."
    }, 200
