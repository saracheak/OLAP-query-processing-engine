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
        self.cust = ''
        self.sum_1_quant = 0
        self.count_1_quant = 0
        self.max_1_quant = 0
        self.min_1_quant = 0
        self.avg_1_quant = 0
        self.avg_1_quant_sum = 0
        self.avg_1_quant_count = 0
        self.sum_2_quant = 0
        self.sum_3_quant = 0
        self.avg_3_quant = 0
        self.avg_3_quant_sum = 0
        self.avg_3_quant_count = 0

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

#Scan for grouping variable 1
cur.execute("SELECT * FROM sales;")

for row in cur:
    group_values = []
    for v in GROUPING_ATTRIBUTES:
        group_values.append(row[COLUMN_INDEX[v]])

    group_key = tuple(group_values)

    if row[COLUMN_INDEX["state"]] == "NY":
        mf_struct[group_key].sum_1_quant += row[COLUMN_INDEX["quant"]]
        mf_struct[group_key].count_1_quant += 1
        if mf_struct[group_key].max_1_quant == 0 or row[COLUMN_INDEX["quant"]] > mf_struct[group_key].max_1_quant: 
            mf_struct[group_key].max_1_quant = row[COLUMN_INDEX["quant"]]
        if mf_struct[group_key].min_1_quant == 0 or row[COLUMN_INDEX["quant"]] < mf_struct[group_key].min_1_quant:
            mf_struct[group_key].min_1_quant = row[COLUMN_INDEX["quant"]]
        mf_struct[group_key].avg_1_quant_sum += row[COLUMN_INDEX["quant"]]
        mf_struct[group_key].avg_1_quant_count += 1

#Scan for grouping variable 2
cur.execute("SELECT * FROM sales;")

for row in cur:
    group_values = []
    for v in GROUPING_ATTRIBUTES:
        group_values.append(row[COLUMN_INDEX[v]])

    group_key = tuple(group_values)

    if row[COLUMN_INDEX["state"]] == "NJ":
        mf_struct[group_key].sum_2_quant += row[COLUMN_INDEX["quant"]]


#Scan for grouping variable 3
cur.execute("SELECT * FROM sales;")

for row in cur:
    group_values = []
    for v in GROUPING_ATTRIBUTES:
        group_values.append(row[COLUMN_INDEX[v]])

    group_key = tuple(group_values)

    if row[COLUMN_INDEX["state"]] == "CT":
        mf_struct[group_key].sum_3_quant += row[COLUMN_INDEX["quant"]]
        mf_struct[group_key].avg_3_quant_sum += row[COLUMN_INDEX["quant"]]
        mf_struct[group_key].avg_3_quant_count += 1

#Finalize AVG values
for group_key, entry in mf_struct.items():
        
        if entry.avg_1_quant_count != 0:
            entry.avg_1_quant = entry.avg_1_quant_sum / entry.avg_1_quant_count        
        if entry.avg_3_quant_count != 0:
            entry.avg_3_quant = entry.avg_3_quant_sum / entry.avg_3_quant_count

#Finalize HAVING values
HAVING_CONDITIONS = entry.sum_1_quant > 2 * entry.sum_2_quant and entry.avg_1_quant > entry.avg_3_quant
filtered_groups = []
for group_key, entry in mf_struct.items():
    if eval("entry.sum_1_quant > 2 * entry.sum_2_quant and entry.avg_1_quant > entry.avg_3_quant"):
        filtered_groups.append((group_key, entry))
        

print("\n\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = ['cust', 'sum_1_quant', 'count_1_quant', 'max_1_quant', 'min_1_quant', 'sum_2_quant', 'sum_3_quant', 'avg_1_quant', 'avg_3_quant']
# Print table header
header = "group_key".ljust(20)
for attr in SELECT_ATTRIBUTES:
    header += attr.ljust(20)
print(header)
print("-" * len(header))

# Print one row per group
final_groups = ""
if HAVING_CONDITIONS != "":
    final_groups = filtered_groups
else:
    final_groups = mf_struct.items()

for group_key, entry in final_groups:
    row_output = str(group_key).ljust(20)

    for attr in SELECT_ATTRIBUTES:
        row_output += str(getattr(entry, attr)).ljust(20)

    print(row_output)
    #remove the two lines below if the table output function is no longer at the bottom of the output code file
cur.close() 
conn.close()
    