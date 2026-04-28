"""
Team Members:
- Ravisara Cheakdkaipejchara 20034203
- Shasha Alvares 20033405

This file is the main query processing engine

Each function must have:
    - function header
    - docstrings to explain what the function should do and return
    - inline comments when necessary

"""
import os

class Generator:
    """
    This class contains functions that i) interact with the user, or ii) produce output code
    """

    def read_input_to_phi(input_file):
        """
        This function reads the input file and converts it into a structure with the phi parameters

        :returns: dictionary with the 6 phi parameters and the corresponding values
        :rtype: dict {str: [str]}
        """
        try:
            with open(input_file, "r") as file:
                lines = file.readlines()
        except Exception as e:
            exit(1) #If the input file is not found, the program can't run
        
        phi_params = {"S": [], "n": [], "V": [], "F": [], "p": [], "G": []} #sigma = p (predicate)
        headers = [
            "SELECT ATTRIBUTE",
            "NUMBER OF GROUPING VARIABLES",
            "GROUPING ATTRIBUTES",
            "F-VECT",
            "SELECT CONDITION-VECT",
            "HAVING CONDITION"
        ]
        for line in lines:
            if not line: continue #skip any empty lines
            line = line.strip()
            if line.startswith("SELECT ATTRIBUTE"):
                param = "S"
                line = line.split(":", 1)[1]
            elif line.startswith("NUMBER OF GROUPING"):
                param = "n"
                line = line.split(":", 1)[1]
            elif line.startswith("GROUPING ATTRIBUTES"):
                param = "V"
                line = line.split(":", 1)[1]
            elif line.startswith("F-VECT"):
                param = "F"
                line = line.split(":", 1)[1]
            elif line.startswith("SELECT CONDITION-VECT"):
                param = "p"
                line = line.split(":", 1)[1]
            elif line.startswith("HAVING_CONDITION"):
                param = "G"
                line = line.split(":", 1)[1]
            
            if param and line:
                if param in ["S", "V", "F"]: #these variables were provided in input as a single comma separated line
                    parts = line.split(",")
                    phi_params[param].extend([p.strip() for p in parts if p.strip()])   #extend flattens list
                else:
                    phi_params[param].append(line)
        return phi_params
    
            
    def user_input_to_phi(param_S, param_n, param_V, param_F, param_p, param_G):
        """
        This function takes the user arguments, cleans them, and converts it into a structure with the phi parameters

        :returns: dictionary with the 6 phi parameters and the corresponding values
        :rtype: dict {str: [str]}
        """
        phi_params = {"S": [], "n": [], "V": [], "F": [], "p": [], "G": []}

        #these are params that are definitely single-values, so we can just add directly
        phi_params["n"].append(param_n)
        phi_params["G"].append(param_G)

        map = {
            "S": param_S,
            "V": param_V,
            "F": param_F,
            "p": param_p
        }
        #these params are possibly multi-valued, so we have to add this way
        for param in ["S", "V", "F", "p"]:      #these variables were provided in input as a single comma separated line
            actual_value = map[param]           #get the argument
            parts = actual_value.split(",")
            phi_params[param].extend([p.strip() for p in parts if p.strip()])   #extends flattens list
        
        return phi_params
    
    
    def generate_import_and_connection():
        """
        This function generates the PostgreSQL code that we want in the output code file in order to connect to the database

        :returns: code for connecting to PostgreSQL database
        :rtype: string
        """
        return """import psycopg2
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
"""

    def first_scan_create_groups(phi_params):
        """
        This function generates the first scan code that create groups based on the grouping on the grouping attributes in V.

        :returns: mf structure for the groups
        :rtype: string
        """
        grouping_attributes = phi_params["V"]
        
        return f"""COLUMN_INDEX = {{
    "cust": 0,
    "prod": 1,
    "day": 2,
    "month": 3,
    "year": 4,
    "state": 5,
    "quant": 6,
    "date": 7 }}
    
GROUPING_ATTRIBUTES = {grouping_attributes}
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
"""
    
    def generate_aggregates_scanning_code(phi_params):
        '''
        Generates code for aggregate functions in F from phi_params.
        Complexity needs to be improved, but currently works on 
        simple params like 
        1. state='NY'
        2. state='NJ'

        :returns: code for calculating specified aggregate (sum, count, max, min, avg)
        :rtype: string
        '''

        """
        This function is used to split the predicate based on the operation
        Example: state!='NY' becomes {'state', '!=', 'NY'}

        :returns: value to the left of operation sign, operation sign, and value to the right of the operation sign
        :rtype: string
        """
        def split_condition(condition_part):
            """
            cleans CONDITION-VECT ([σ]) values
            """
            operators = ["!=", "<=", ">=", "==", "<", ">"]
            for op in operators:
                if op in condition_part:
                    left, right = condition_part.split(op) #dynamically splits string around operation sign
                    column = left.strip()
                    value = right.strip().replace("'", "").replace("’", "") #get rid of extra quotes around value
                    return column, op, value

        f_vect = phi_params["F"]
        predicates = phi_params["p"]

        #required instead of returning code block bc we add to the block
        #depending on what functions the user chose 
        code = "" 

        for p in predicates: 
            #ensures calculations are done for each predicate
            #example: 1.state='NY'

            group_var = p.split(".")[0].strip() #splits grouping varible number, so based on the example above group_var = 1
            condition_part = p.split(".", 1)[1] #condition part = "state='NY'"
            column_name, operator, expected_value = split_condition(condition_part)

            #need a variable to store code rather than returning at at once bc the functions added are 
            #dependent on the the query given
            code+=f"""
#Scan for grouping variable {group_var}
cur.execute("SELECT * FROM sales;")

for row in cur:
    group_values = []
    for v in GROUPING_ATTRIBUTES:
        group_values.append(row[COLUMN_INDEX[v]])

    group_key = tuple(group_values)

    if row[COLUMN_INDEX["{column_name}"]] {operator} "{expected_value}":
"""
            for agg in f_vect:
                agg_func, agg_group_var, agg_column = HelperFunctions.parse_agg_names(agg)

                if agg_group_var == group_var:
                    if agg_func == "sum":
                        code+= f"""        mf_struct[group_key].{agg} += row[COLUMN_INDEX["{agg_column}"]]\n"""
                    elif agg_func == "count":
                        code+=f"""        mf_struct[group_key].{agg} += 1\n"""
                    elif agg_func == "max":
                        code += f"""        if mf_struct[group_key].{agg} == 0 or row[COLUMN_INDEX["{agg_column}"]] > mf_struct[group_key].{agg}: 
            mf_struct[group_key].{agg} = row[COLUMN_INDEX["{agg_column}"]]\n"""
                    elif agg_func == "min":
                        code += f"""        if mf_struct[group_key].{agg} == 0 or row[COLUMN_INDEX["{agg_column}"]] < mf_struct[group_key].{agg}:
            mf_struct[group_key].{agg} = row[COLUMN_INDEX["{agg_column}"]]\n"""
                    elif agg_func == "avg":
                        code += f"""        mf_struct[group_key].{agg}_sum += row[COLUMN_INDEX["{agg_column}"]]
        mf_struct[group_key].{agg}_count += 1"""
            
            code +="\n"
        return code
    
    
    def generate_final_avg(phi_params):
        """
        This function will calculate the average for the grouping variables with 'avg', after all 
        the scans are complete and the sum and count for those variables were calculated

        :returns: string of avg for each grouping variable
        """

        f_vect = phi_params["F"]
        code = """
#Finalize AVG values
for group_key, entry in mf_struct.items():
"""
        has_avg = False

        for agg in f_vect:
            agg_func, group_var, column = HelperFunctions.parse_agg_names(agg)
            if agg_func == "avg":
                has_avg=True
                code += f"""        
        if entry.{agg}_count != 0:
            entry.{agg} = entry.{agg}_sum / entry.{agg}_count"""
        
        if has_avg == False:
            return ''
        return code + "\n"
    
    def generate_having_condition(phi_params):
        """
        This function filters the output for only the rows that satisfy the having condition.
        It is run after all the scans are complete.

        :returns: filters for each having condition
        :rtype: string
        """
        f_vect = phi_params["F"]
        having_conditions = phi_params["G"][0]

        #only run if there is a having condition
        if having_conditions != "":
            
            translated_having = having_conditions
            for f in f_vect:
                translated_having = translated_having.replace(f, f"entry.{f}")
            translated_having = translated_having.replace("OR", "or").replace("AND", "and") #handles capitalised or lowercase AND/OR
            
            code = f"""
#Finalize HAVING values
HAVING_CONDITIONS = {translated_having}
filtered_groups = []
for group_key, entry in mf_struct.items():
    if eval("{translated_having}"):
        filtered_groups.append((group_key, entry))
        """
        return code + '\n'
    pass

    def generate_output_test(phi_params):
        """
        This function is for debugging and generates output to confirm that the function first_scan_create_groups(phi_params) successfully created the groups.
        It can be deleted later once we don't need to debug
        """
        selected_attributes = phi_params["S"]

        return f"""
print("\\n\\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = {selected_attributes}
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
    """
    
    def generate_full_program(phi_params):
        return (
        Generator.generate_import_and_connection()
        + HelperFunctions.convert_to_mf_struct(phi_params)
        + Generator.first_scan_create_groups(phi_params)
        + Generator.generate_aggregates_scanning_code(phi_params)
        + Generator.generate_final_avg(phi_params)
        + Generator.generate_having_condition(phi_params)
        + Generator.generate_output_test(phi_params) #this is for debugging the creation of groups and can be deleted later
    )

class HelperFunctions:
    """
    This class contains all the helper functions for class Generator. These functions don't produce any code
    """

    def write_to_file(file_content):
        """
        This function takes in any string and writes it to another file 
        """
        try:
            with open("example_output.py", "w") as file:
                file.write(file_content)
        except:
            print("Could not write to file")
            exit(1)

    def validate_multi_value_input_string(multi_value_param):
        """
        This function validates the user typed input variable is entered in the correct comma seperated 
        format and does not contain any empty values. They are re-prompted if their input is not in the
        correct format.

        :returns: multi_value_param
        :rtype: string
        """
        #split the input string based on commas
        while True:
            parts = [p.strip() for p in multi_value_param.split(',')]
            
            if parts[0]=='':
                print("Invalid input: at least one input must be provided.")
                multi_value_param = input("Re-enter param:\n").strip()
                continue
            elif len(parts)==1:
                #validate that one input was given, not multiple inputs without commas
                if " " in parts[0]:
                    print("Invalid input: multiple values must be separated by commas.")
                    multi_value_param = input("Re-enter param:\n").strip()
                    continue
                return multi_value_param
            else: #multiple values inputted, verify none were empty like a, ,b
                for p in parts:
                    if (p==''):
                        print("Invalid input: input values cannot be empty.")
                        multi_value_param = input("Re-enter param:\n").strip()
                        continue
                return multi_value_param
            
    def validate_n_is_int(n_param):
        """
        This function validates the user typed input for n is an integer. They are re-prompted if their input is not in the
        correct format.

        :returns: n_param
        :rtype: string
        """
        while True: 
            try:    
                n_type = isinstance(int(n_param), int)
                if(n_type == True):
                    break
            except ValueError:
                print("Invalid input: n parameter must be an integer.")
                n_param = input("Re-enter n param:\n").strip()
        return n_param
    
    def convert_to_mf_struct(phi_params):
        """
        This function takes in the phi_params dictionary
        :returns: string of the mf-struct, with the type definitions and variables
        """

        select_attributes = ""

        #initialise grouping variables with an empty string
        for v in phi_params["V"]:
            select_attributes += f"        self.{v} = ''\n" #4 spaces replaces tabs bc you cannot mix space and tabs in python


        for f in phi_params["F"]:
            select_attributes += f"        self.{f} = 0\n" #initialise aggregate functions (avg, min, max, count, sum) with 0

            #avg is calculated from sum/count, so sum and count need to be initialized with it. 
            #in MF struct avg initialization will show up as (for example): 
                #avg_1_quant = 0  (initialized above)
                #avg_1_quant_sum = 0 (initialized below)
                #avg_1_quant_count = 0 (initialized below)
            agg_func, group_var, column = HelperFunctions.parse_agg_names(f)
            if agg_func == "avg":
                select_attributes += f"        self.{f}_sum = 0\n"
                select_attributes += f"        self.{f}_count = 0\n"
        
        mf_struct = f"""\nclass MFStruct:
    def __init__(self):
{select_attributes}
mf_struct ={{}}\n"""
        return mf_struct
    
    def parse_agg_names(agg_name):
        """
        This function parses the aggregate name into the aggregate, number, and column
        Example Output:
        sum_1_quant -> ("sum", "1", "quant")
        avg_3_quant -> ("avg", "3", "quant")

        :returns: 3 strings with the corresponding aggregate, number, and column name from the input
        :rtype: string
        """
        parts = agg_name.split("_")
        agg_func = parts[0] #sum, count, max, min, avg
        group_var = parts[1] #, 1, 2, 3, etc. 
        column = parts[2] #cust, prod, state, etc.

        return agg_func, group_var, column


if __name__ == "__main__":
    while True:
        option = input("Do you want to input phi parameters by file or user input? Enter 'file' or 'user':\n")
        option = option.lower()
        if option == "file":
            filename = input("Enter file path:\n") 
            #continue to reprompt user until existing filename is given
            while True:
                if not os.path.exists(filename):
                    print("Input filename does not exist.")
                    filename = input("Enter file path:\n")
                break
            phi_params = Generator.read_input_to_phi(filename)
            #mf_struct_string = Generator.convert_to_mf_struct(phi_params)
            #HelperFunctions.write_to_file(mf_struct_string)
            mf_struct_string = Generator.generate_full_program(phi_params)
            HelperFunctions.write_to_file(mf_struct_string)
            break
        elif option == "user":
            print("Ensure multi-valued parameters are separated by ','\n")

            param_S = HelperFunctions.validate_multi_value_input_string(input("Enter S param:\n").strip())
            param_n = HelperFunctions.validate_n_is_int(input("Enter n param:\n").strip())
            param_V = HelperFunctions.validate_multi_value_input_string(input("Enter V param:\n").strip())
            param_F = HelperFunctions.validate_multi_value_input_string(input("Enter F param:\n").strip())
            param_p = HelperFunctions.validate_multi_value_input_string(input("Enter sigma param:\n").strip())
            
            #IDK what to do for input validation here :/
            param_G = input("Enter G param:\n").strip()

            print(Generator.user_input_to_phi(param_S, param_n, param_V, param_F, param_p, param_G))
            break
        else:
            print("Invalid input. Please enter 'file' or 'user")