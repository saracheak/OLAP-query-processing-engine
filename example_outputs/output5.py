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
        self.sum_X_quant = 0
        self.sum_Y_quant = 0
        self.sum_Z_quant = 0

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
    
GROUPING_ATTRIBUTES = ['prod']
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

#Scan for grouping variable X
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        prod, = group_key
    
        if ((float(row[COLUMN_INDEX['month']]) == float(1)) if str(row[COLUMN_INDEX['month']]).replace('.','',1).isdigit() and str(1).replace('.','',1).isdigit() else (row[COLUMN_INDEX['month']] == 1)) and row[COLUMN_INDEX['prod']] == prod:
            mf_struct[group_key].sum_X_quant += row[COLUMN_INDEX["quant"]]


#Scan for grouping variable Y
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        prod, = group_key
    
        if ((float(row[COLUMN_INDEX['month']]) == float(2)) if str(row[COLUMN_INDEX['month']]).replace('.','',1).isdigit() and str(2).replace('.','',1).isdigit() else (row[COLUMN_INDEX['month']] == 2)) and row[COLUMN_INDEX['prod']] == prod:
            mf_struct[group_key].sum_Y_quant += row[COLUMN_INDEX["quant"]]


#Scan for grouping variable Z
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        prod, = group_key
    
        if ((float(row[COLUMN_INDEX['month']]) == float(3)) if str(row[COLUMN_INDEX['month']]).replace('.','',1).isdigit() and str(3).replace('.','',1).isdigit() else (row[COLUMN_INDEX['month']] == 3)) and row[COLUMN_INDEX['prod']] == prod:
            mf_struct[group_key].sum_Z_quant += row[COLUMN_INDEX["quant"]]


print("\n\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = ['prod', 'sum_X_quant', 'sum_Y_quant', 'sum_Z_quant']
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
    