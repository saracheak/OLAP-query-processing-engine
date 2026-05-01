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

#Each 'row' is a tuple representing one record; columns are accessed by index within the row
for row in cur:
    group_values = []

    #Add each column from the database to group_values that are specified in v. 
    #For example row[COLUMN_INDEX["cust"]]===row[0] and adds the first value of every row to group_values. 
    for v in GROUPING_ATTRIBUTES: 
        group_values.append(row[COLUMN_INDEX[v]])
    
    group_key = tuple(group_values) 

    #if the group does not already exist, the group key becomes the dictionary key in mfstruct. This ensures we get every combination of group keys
    if group_key not in mf_struct: 
        mf_struct[group_key] = MFStruct() 

        # Store the grouping attribute value (e.g., cust = "Dan") inside this group's MFStruct object
        #Example: self.cust = '' initially. After self.cust = 'Dan'
        for v in GROUPING_ATTRIBUTES: 
            setattr(mf_struct[group_key], v, row[COLUMN_INDEX[v]])

#Scan for grouping variable 1
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        cust, = group_key
    
        if row[COLUMN_INDEX['state']] == 'NY' and row[COLUMN_INDEX['cust']] == cust:
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

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        cust, = group_key
    
        if row[COLUMN_INDEX['state']] == 'NJ' and row[COLUMN_INDEX['cust']] == cust:
            mf_struct[group_key].sum_2_quant += row[COLUMN_INDEX["quant"]]


#Scan for grouping variable 3
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        cust, = group_key
    
        if row[COLUMN_INDEX['state']] == 'CT' and row[COLUMN_INDEX['cust']] == cust:
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
HAVING_CONDITIONS = entry.sum_1_quant > 2 * entry.sum_2_quant or entry.avg_1_quant > entry.avg_3_quant
filtered_groups = []
for group_key, entry in mf_struct.items():
    if eval("entry.sum_1_quant > 2 * entry.sum_2_quant or entry.avg_1_quant > entry.avg_3_quant"): #this evaluates the having condition, if it satisfies it will be added to 'filtered_groups'
        filtered_groups.append((group_key, entry))
        

print("\n\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = ['cust', 'sum_1_quant', 'count_1_quant', 'max_1_quant', 'min_1_quant', 'sum_2_quant', 'sum_3_quant', 'avg_1_quant', 'avg_3_quant']
# Print table header
header = "group_key".ljust(20)
for attr in SELECT_ATTRIBUTES:
    header += attr.ljust(20)
print(header)
print("-" * len(header))

#if there is a HAVING condition, use the filtered groups NOT the mf struct
final_groups = ""
if True: #this flag is True if there was a HAVING, False if no HAVING condition
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
    