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

    cur = conn.cursor() #prepares to run the queries
        
    cur.execute("SELECT version();")
    db_version = cur.fetchone() #retrieves data row by row equivalent to: "for row in cur:"
    print(f"Connected to: {db_version}")
    cur.execute("SELECT * FROM sales;")  #execute sends the SQL query to PostgreSQL, and the columns retrieved are stored in the cursor


    cur.close()
    conn.close()
except Exception:
    print("Failed to connect to the database")
    exit(1)