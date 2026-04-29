import psycopg2
try:
    conn = psycopg2.connect(
        dbname="sales",
        user="postgres",
        password="password", #you may need to change your postgres password using ALTER USER postgres WITH PASSWORD 'password';
        host="localhost"
    )

    cur = conn.cursor() #prepares to run the queries
        
    #cur.close()
    #conn.close()
except Exception:
    print("Failed to connect to the database")
    exit(1)

class MFStruct:
    def __init__(self):
        self.prod = ''
        self.month = ''
        self.avg_X_quant = 0
        self.avg_X_quant_sum = 0
        self.avg_X_quant_count = 0
        self.avg_Y_quant = 0
        self.avg_Y_quant_sum = 0
        self.avg_Y_quant_count = 0

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
    
GROUPING_ATTRIBUTES = ['prod', 'month']
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

#Scan for grouping variable X
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        prod, month = group_key
    
        if row[COLUMN_INDEX['prod']] == prod and row[COLUMN_INDEX['month']] < month:
            mf_struct[group_key].avg_X_quant_sum += row[COLUMN_INDEX["quant"]]
            mf_struct[group_key].avg_X_quant_count += 1


#Scan for grouping variable Y
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        prod, month = group_key
    
        if row[COLUMN_INDEX['prod']] == prod and row[COLUMN_INDEX['month']] > month:
            mf_struct[group_key].avg_Y_quant_sum += row[COLUMN_INDEX["quant"]]
            mf_struct[group_key].avg_Y_quant_count += 1


#Finalize AVG values
for group_key, entry in mf_struct.items():
        
    if entry.avg_X_quant_count != 0:
        entry.avg_X_quant = entry.avg_X_quant_sum / entry.avg_X_quant_count        
    if entry.avg_Y_quant_count != 0:
        entry.avg_Y_quant = entry.avg_Y_quant_sum / entry.avg_Y_quant_count

print("\n\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = ['prod', 'month', 'avg_X_quant', 'avg_Y_quant']
# Print table header
header = "group_key".ljust(20)
for attr in SELECT_ATTRIBUTES:
    header += attr.ljust(20)
print(header)
print("-" * len(header))

#if there is a HAVING condition, use the filtered groups NOT the mf struct
final_groups = ""
if False: #this flag is True if there was a HAVING, False if no HAVING condition
    final_groups = filtered_groups
else:
    final_groups = mf_struct.items()

# Print one row per group
for group_key, entry in final_groups:
    row_output = str(group_key).ljust(20)

    for attr in SELECT_ATTRIBUTES:
        row_output += str(getattr(entry, attr)).ljust(20)

    print(row_output)
    #remove the two lines below if the table output function is no longer at the bottom of the output code file
cur.close() 
conn.close()
    