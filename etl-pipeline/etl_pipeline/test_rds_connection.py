import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_rds_connection():
    conn = None
    try:
        print(f"Attempting to connect to RDS host: {os.getenv('RDS_HOST')}")
        conn = psycopg2.connect(
            host=os.getenv('RDS_HOST'),
            database=os.getenv('RDS_DB_NAME'),
            user=os.getenv('RDS_USERNAME'),
            password=os.getenv('RDS_PASSWORD'),
            connect_timeout=30  # Increased timeout
        )
        print("Successfully connected to the RDS database!")
        
        cur = conn.cursor()
        cur.execute("SELECT version();")
        db_version = cur.fetchone()
        print(f"PostgreSQL database version: {db_version[0]}")

        cur.close()
    except psycopg2.OperationalError as e:
        print(f"Unable to connect to the database: {e}")
        print(f"RDS Host: {os.getenv('RDS_HOST')}")
        print(f"RDS Database: {os.getenv('RDS_DB_NAME')}")
        print(f"RDS Username: {os.getenv('RDS_USERNAME')}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    test_rds_connection()