"""
Just for testing the connection code works, this should not be a separate file and should be a template string in engine.py
"""

import psycopg2
try:
    conn = psycopg2.connect(
        dbname="sales",
        user="postgres",
        password="password", #you may need to change your postgres password using ALTER USER postgres WITH PASSWORD 'password';
        host="localhost"
    )

    cur = conn.cursor()
        
    cur.execute("SELECT version();")
    db_version = cur.fetchone()
    print(f"Connected to: {db_version}")

    cur.close()
    conn.close()
except Exception:
    print("Failed to connect to the database")
    exit(1)