import os
import subprocess
import time
import psycopg2  # PostgreSQL Python client

def execute_sql(request):
    """Cloud Function entry point."""
    # Configuration via environment variables
    instance_connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "cloud-sql-iam-user")  # IAM-authenticated user
    query = os.getenv("SQL_QUERY", "SELECT 1;")  # Default query

    try:
        # Download Cloud SQL Proxy (if not bundled)
        if not os.path.exists("/tmp/cloud_sql_proxy"):
            subprocess.run(
                [
                    "curl", "-o", "/tmp/cloud_sql_proxy",
                    "https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64"
                ],
                check=True
            )
            subprocess.run(["chmod", "+x", "/tmp/cloud_sql_proxy"], check=True)

        # Start Cloud SQL Proxy with IAM authentication
        proxy_command = [
            "/tmp/cloud_sql_proxy",
            f"-instances={instance_connection_name}=tcp:5432",
            "--auto-iam-authn"
        ]
        proxy_process = subprocess.Popen(proxy_command)
        time.sleep(5)  # Wait for the proxy to start

        # Connect to the database using psycopg2
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            host="127.0.0.1",
            port=5432
        )
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        conn.close()

        # Stop the Cloud SQL Proxy
        proxy_process.terminate()

        return {
            "status": "success",
            "results": results
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

