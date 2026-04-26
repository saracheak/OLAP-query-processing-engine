"""
This file is the main query processing engine

Each function must have:
    - function header
    - docstrings to explain what the function should do and return
    - inline comments when necessary
"""

"""
Read the input and generate a program to create the ‘mf-structure’
(in memory) – i.e., the generated program contains a list of
type/class definitions and variables for the ‘mf-structure’.
"""
import os

class Generator:
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
        This function generates the PostgreSQL code that we want 
        in the output code file in order to connect to the database

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
        
    cur.execute("SELECT version();")
    db_version = cur.fetchone() #retrieves data row by row equivalent to: "for row in cur:"
    print(f"Connected to: {db_version}")

    #cur.close()
    #conn.close()
except Exception:
    print("Failed to connect to the database")
    exit(1)
"""

    def first_scan_create_groups(phi_params):
        """
        This function generates the first scan code that create groups based on the 
        grouping on the grouping attributes in V.

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
    
    def generate_output_test(phi_params):
        """
        This function is for debugging and generates output to confirm
        that the function first_scan_create_groups(phi_params) successfully created the groups.
        It can be deleted later once we don't need to debug
        """
        selected_attributes = phi_params["S"]

        return f"""print("\\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = {selected_attributes}
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
    """

    def convert_to_mf_struct(phi_params):
        """
        This function takes in the phi_params dictionary
        :returns: string of the mf-struct, with the type definitions and variables
        """

        select_attributes = ""

        #initialise grouping variables with an empty string
        for v in phi_params["V"]:
            select_attributes += f"        self.{v} = ''\n" #4 spaces replaces tabs bc you cannot mix space and tabs in python

        #initialise aggregate functions (avg, min, max) with 0
        for f in phi_params["F"]:
            select_attributes += f"        self.{f} = 0\n"
        
        mf_struct = f"""\nclass MFStruct:
    def __init__(self):
{select_attributes}
mf_struct ={{}}\n"""
        return mf_struct
    
    def generate_full_program(phi_params):
        return (
        Generator.generate_import_and_connection()
        + Generator.convert_to_mf_struct(phi_params)
        + Generator.first_scan_create_groups(phi_params)
        + Generator.generate_output_test(phi_params) #this is for debugging the creation of groups and can be deleted later
    )

class HelperFunctions:
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

            param_S = Generator.validate_multi_value_input_string(input("Enter S param:\n").strip())
            param_n = Generator.validate_n_is_int(input("Enter n param:\n").strip())
            param_V = Generator.validate_multi_value_input_string(input("Enter V param:\n").strip())
            param_F = Generator.validate_multi_value_input_string(input("Enter F param:\n").strip())
            param_p = Generator.validate_multi_value_input_string(input("Enter sigma param:\n").strip())
            
            #IDK what to do for input validation here :/
            param_G = input("Enter G param:\n").strip()

            print(Generator.user_input_to_phi(param_S, param_n, param_V, param_F, param_p, param_G))
            break
        else:
            print("Invalid input. Please enter 'file' or 'user")