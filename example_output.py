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

    #cur.close()
    #conn.close()
except Exception:
    print("Failed to connect to the database")
    exit(1)

class MFStruct:
    def __init__(self):
        self.cust = ''
        self.sum_1_quant = 0
        self.avg_1_quant = 0
        self.sum_2_quant = 0
        self.sum_3_quant = 0
        self.avg_3_quant = 0

mf_struct ={}
COLUMN_INDEX = {
    "cust": 0,
    "prod": 1,
    "day": 2,
    "month": 3,
    "year": 4,
    "state": 5,
    "quant": 6,
    "date": 7 }
    
GROUPING_ATTRIBUTES = ['cust']
cur.execute("SELECT * FROM sales;") #execute sends the SQL query to PostgreSQL, and the columns retrieved are stored in the cursor

for row in cur:
    group_values = []

    for v in GROUPING_ATTRIBUTES:
        group_values.append(row[COLUMN_INDEX[v]])
    
    group_key = tuple(group_values)

    if group_key not in mf_struct:
        mf_struct[group_key] = MFStruct()

        for v in GROUPING_ATTRIBUTES:
            setattr(mf_struct[group_key], v, row[COLUMN_INDEX[v]])
print("\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = ['cust', 'sum_1_quant', 'sum_2_quant', 'sum_3_quant']
# Print table header
header = "group_key".ljust(20)
for attr in SELECT_ATTRIBUTES:
    header += attr.ljust(20)
print(header)
print("-" * len(header))

# Print one row per group
for group_key, entry in mf_struct.items():
    row_output = str(group_key).ljust(20)

    for attr in SELECT_ATTRIBUTES:
        row_output += str(getattr(entry, attr)).ljust(20)

    print(row_output)
    