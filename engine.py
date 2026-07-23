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
    Compiles phi parameters into MF query-processing Python source.
    """

    def parse_phi_lines(lines):
        """
        Parse phi parameters from an iterable of text lines.

        :returns: dictionary with the 6 phi parameters and the corresponding values
        :rtype: dict {str: [str]}
        """
        phi_params = {"S": [], "n": [], "V": [], "F": [], "p": [], "G": []} #sigma = p (predicate)
        param = None
        for line in lines:
            if not line:
                continue #skip any empty lines
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

    def parse_phi_text(phi_text):
        """
        Parse phi parameters from a raw text string.

        :returns: dictionary with the 6 phi parameters and the corresponding values
        :rtype: dict {str: [str]}
        """
        return Generator.parse_phi_lines(phi_text.splitlines())

    def read_input_to_phi(input_file):
        """
        Read a phi-parameter file and convert it into a structure with the phi parameters.

        :returns: dictionary with the 6 phi parameters and the corresponding values
        :rtype: dict {str: [str]}
        """
        try:
            with open(input_file, "r") as file:
                lines = file.readlines()
        except Exception as e:
            raise FileNotFoundError(f"Could not read input file: {input_file}") from e
        return Generator.parse_phi_lines(lines)

    def params_to_phi(param_S, param_n, param_V, param_F, param_p, param_G):
        """
        Build a phi-parameter structure from individual argument values.

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

    # Backwards-compatible alias
    user_input_to_phi = params_to_phi
    
    
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
        This function generates the first scan code that create groups based on the grouping of the grouping attributes in V.
        It creates one MFStruct entry for each unique group based on the grouping attributes.

        :returns: mf structure for the groups
        :rtype: string
        """
        grouping_attributes = phi_params["V"]
        
        #In the setup_sales.sql database, each row contains one record, which is a tuple of values. 
        #Example: row1 = ("Dan", "Apple", 5, 3, 2023, "NY", 100, "2023-03-01")
        #         row2 = ("Claire", "Milk", 5, 3, 2023, "NY", 100, "2023-03-01")

        #We are looping through the rows using the cursor and picking specific column values to assess. We have to index
        #into each row to find a specific column:
        #Examples: row1[0] = "Dan", row1[1] = "Apple"
        #          row2[0] = "Claire", row2[1] = "Milk"

        #Instead of remembering row[0] = "cust" and row[1] = "product", we created the COLUMN_INDEX dictionary so we can just refer 
        #to the column name itself in our code. This makes the code clearer to read and write. 

        #In our code: "row[COLUMN_INDEX["cust"]]" === row[0]="cust"
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
"""
    
    def generate_aggregates_scanning_code(phi_params):
        """
        Generates code for aggregate functions in F from phi_params. Includes processing for CONDITION-VECT([σ]) conditions.
        
        :returns: code for calculating specified aggregate (sum, count, max, min, avg)
        :rtype: string
        """
        grouping_attributes = phi_params["V"]
        f_vect = phi_params["F"]
        predicates = phi_params["p"]
        # print(predicates)

        #grouping attributes is currently a tuple so we make it a string
        if len(grouping_attributes) == 1:
            grouping_attributes_str = f"{grouping_attributes[0]},"
        else:
            grouping_attributes_str = ", ".join(grouping_attributes)

        #required instead of returning code block bc we add to the block
        #depending on what functions the user chose 
        code = ""

        for p in predicates: 
            #processing for each predicate, e.g. "X.state='NY' and X.month=month," becomes group_var = "X", condition_part = "state=='NY' and month==month"
            group_var, condition_part = HelperFunctions.process_predicate(p)

            #splits predicate into e.g. [{"column": "state", "op": "==", "value": "'NY'"}, {"column": "month", "op": "==", "value": "month"}]
            predicate_list = HelperFunctions.split_predicate(condition_part)

            #need a variable to store code rather than returning at at once bc the functions added are dependent on the the query given
            code+=f"""
#Scan for grouping variable {group_var}
cur.execute("SELECT * FROM sales;")

for row in cur:

    for group_key, entry in mf_struct.items():
        # Unpack the 'anchor' values for this group
        {grouping_attributes_str} = group_key
    
"""
            #go through predicate_list and put each in the format of row[COLUMN_INDEX['{column}']] {op} {value} and append to code
            full_condition = [] #each element looks like this: f"row[COLUMN_INDEX['{column}']] {op} {value}")
            processed_attributes = []

            #This for loop goes through each of the predicates provided by the user (e.g. X.state = 'NY' or X.month < month)
            for pred in predicate_list:
                column, op, value = pred["column"], pred["op"], pred["value"]

                # If the value is one of our grouping attributes, it refers to the current group's value, includes if X.month < month
                if value in grouping_attributes:
                    value = value
                    processed_attributes.append(value)
                # If value in the input is a digit, e.g. X.month = 1
                elif value.isdigit():
                    value = value
                else:
                    # Otherwise, it's a literal like 'NY' or 2020
                    value = f"'{value}'"

                lhs = f"row[COLUMN_INDEX['{column}']]"
                condition = (
                    f"(float({lhs}) {op} float({value})) "
                    f"if str({lhs}).replace('.','',1).isdigit() and str({value}).replace('.','',1).isdigit() "
                    f"else ({lhs} {op} {value})"
                )
                
                full_condition.append(f"({condition})")
            
            #This for loop goes through each of the predicates NOT provided by the user but is implicitely understood, we have to add an equality check
            #e.g. adds row.cust = cust. This makes sure that the data doesn't go into other rows, so e.g. only Dan's rows are updated
            #We do this because we can't just add an equality check to every row (in previous for loop), because then we wouldn't be able to do emf queries
            #such as moving averages
            for attr in grouping_attributes:
                if attr not in processed_attributes:
                    full_condition.append(f"row[COLUMN_INDEX['{attr}']] == {attr}")


            #full_condition includes all the predicates (user provided + implicit)
            # E.g. row[COLUMN_INDEX['{column}']] {op} {value} and row[COLUMN_INDEX['{column}']] {op} {value} etc...
            full_condition = " and ".join(full_condition)
            
            code += f"        if {full_condition}:\n" #add CONDITION-VECT([σ]) if to output.py

            #aggregate function calculations
            for agg in f_vect:
                agg_func, agg_group_var, agg_column = HelperFunctions.parse_agg_names(agg)

                if agg_group_var == group_var:
                    if agg_func == "sum":
                        code+= f"""            mf_struct[group_key].{agg} += row[COLUMN_INDEX["{agg_column}"]]\n"""
                    elif agg_func == "count":
                        code+=f"""            mf_struct[group_key].{agg} += 1\n"""
                    elif agg_func == "max":
                        code += f"""            if mf_struct[group_key].{agg} == 0 or row[COLUMN_INDEX["{agg_column}"]] > mf_struct[group_key].{agg}: 
                mf_struct[group_key].{agg} = row[COLUMN_INDEX["{agg_column}"]]\n"""
                    elif agg_func == "min":
                        code += f"""            if mf_struct[group_key].{agg} == 0 or row[COLUMN_INDEX["{agg_column}"]] < mf_struct[group_key].{agg}:
                mf_struct[group_key].{agg} = row[COLUMN_INDEX["{agg_column}"]]\n"""
                    elif agg_func == "avg":
                        code += f"""            mf_struct[group_key].{agg}_sum += row[COLUMN_INDEX["{agg_column}"]]
            mf_struct[group_key].{agg}_count += 1\n"""
        
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
            agg_func, group_var, column = HelperFunctions.parse_agg_names(agg) # agg_func becomes equal to the aggregate avg, sum, etc.
            if agg_func == "avg":
                has_avg=True
                code += f"""        
    if entry.{agg}_count != 0:
        entry.{agg} = entry.{agg}_sum / entry.{agg}_count"""
        
        if has_avg == False: #no avg were asked to be calculated
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
        if phi_params["G"]:

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
    if eval("{translated_having}"): #this evaluates the having condition, if it satisfies it will be added to 'filtered_groups'
        filtered_groups.append((group_key, entry))
        """
                return code + '\n'
        return ""

    def generate_output_test(phi_params):
        """
        This function is generates an output table to confirm that the selected results are outputted and 
        were filtered by the 1) grouping variables, 2) phi, and 3) having condition (if applicable).
        """
        selected_attributes = phi_params["S"]

        #output variable 'final_groups' depends on whether there is a HAVING clause
        having = phi_params["G"]
        if having and having[0] != "":
            having_exists = True
        else:
            having_exists = False
        

        return f"""
print("\\n\\nProject Output Debugging Table:")

SELECT_ATTRIBUTES = {selected_attributes}
# Print table header
header = "group_key".ljust(20)
for attr in SELECT_ATTRIBUTES:
    header += attr.ljust(20)
print(header)
print("-" * len(header))

#if there is a HAVING condition, use the filtered groups NOT the mf struct
final_groups = ""
if {having_exists}: #this flag is True if there was a HAVING, False if no HAVING condition
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
    """
    
    def generate_full_program(phi_params):
        return (
        Generator.generate_import_and_connection()
        + HelperFunctions.convert_to_mf_struct(phi_params)
        + Generator.first_scan_create_groups(phi_params)
        + Generator.generate_aggregates_scanning_code(phi_params)
        + Generator.generate_final_avg(phi_params)
        + Generator.generate_having_condition(phi_params)
        + Generator.generate_output_test(phi_params)
    )

class HelperFunctions:
    """
    This class contains all the helper functions for class Generator. These functions don't produce any code
    """

    def write_to_file(file_content, output_path=None):
        """
        Writes generated code to a file. CLI convenience helper — not part of compilation.

        :param file_content: generated Python source
        :param output_path: optional destination path; if omitted, writes to
            example_outputs/outputN.py with an auto-incremented N
        :returns: path written to
        :rtype: str
        """
        if output_path is None:
            # write to the output folder with incrementing numbers so the file does not keep overwriting itself
            output_num = 1
            while os.path.exists(f"example_outputs/output{output_num}.py"):
                output_num += 1
            output_path = f"example_outputs/output{output_num}.py"

        with open(output_path, "w") as file:
            file.write(file_content)
        return output_path

    def validate_multi_value_input_string(multi_value_param):
        """
        Validate a comma-separated multi-value parameter (S, V, F, or p).

        :returns: multi_value_param unchanged when valid
        :rtype: string
        :raises ValueError: if the format is invalid
        """
        parts = [p.strip() for p in multi_value_param.split(',')]

        if not parts or parts[0] == '':
            raise ValueError("at least one input must be provided")
        if len(parts) == 1:
            if " " in parts[0]:
                raise ValueError("multiple values must be separated by commas")
            return multi_value_param
        if any(p == '' for p in parts):
            raise ValueError("input values cannot be empty")
        return multi_value_param

    def validate_n_is_int(n_param):
        """
        Validate that n is an integer string.

        :returns: n_param unchanged when valid
        :rtype: string
        :raises ValueError: if n is not an integer
        """
        try:
            int(n_param)
        except ValueError as e:
            raise ValueError("n parameter must be an integer") from e
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
                #avg_1_quant = 0  (initialized above in - for f...)
                #avg_1_quant_sum = 0 (initialized below in - if agg_func...)
                #avg_1_quant_count = 0 (initialized below in - if agg_func...)
            agg_func, group_var, column = HelperFunctions.parse_agg_names(f)
            if agg_func == "avg":
                select_attributes += f"        self.{f}_sum = 0\n"
                select_attributes += f"        self.{f}_count = 0\n"
        
        mf_struct = f"""\nclass MFStruct:
    def __init__(self):
{select_attributes}
mf_struct ={{}}\n"""
        return mf_struct
    
    def process_predicate(predicate):
        """
        Takes "X.state='NY' and X.month=month"
        Returns ("X", "state='NY' and month=month")
        """
        #get the grouping variable, e.g. X
        group_var = predicate.split(".")[0].strip()
        
        #remove internal group_var, therefore condition_part = "state='NY' and month=month"
        condition_part = predicate.replace(f"{group_var}.", "")
        
        return group_var, condition_part
    
    def split_predicate(condition_part):
        """
        This function cleans predicate values by splitting the predicate based on the operation and if there are multiple predicates in one line
        For example:
            condition_part = "state='NY' and month=month"
            returns [{"column": "state", "op": "==", "value": "'NY'"}, {"column": "month", "op": "==", "value": "month"}]
        :returns: each dictionary contains values for column name, operator, value
        :rtype: list of dict
        """
        results = []
        indiv_predicates = [p.strip() for p in condition_part.split(" and ")] #split condition into individual predicates (e.g. for state='NY' and month=month)
        operators = ["!=", "<=", ">=", "==", "=", "<", ">"]
        
        for pred in indiv_predicates:
            for op in operators:
                if op in pred:
                    parts = pred.split(op, 1) #splits by op only once
                    left = parts[0]
                    right = parts[1]
                    column = left.strip()
                    value = right.strip().replace("'", "").replace("’", "") #get rid of extra quotes around value
                    py_op = "==" if op == "=" else op
                    results.append({"column": column, "op": py_op, "value": value})
                    break
        # print(results)
        return results
    
    def parse_agg_names(agg_name):
        """
        This function parses the aggregate name into the aggregate, number, and column
        Example Output:
        sum_1_quant -> "sum", "1", "quant"
        avg_3_quant -> "avg", "3", "quant"

        :returns: 3 strings with the corresponding aggregate, number, and column name from the input
        :rtype: string
        """
        parts = agg_name.split("_")
        agg_func = parts[0] #sum, count, max, min, avg
        group_var = parts[1] #, 1, 2, 3, etc. 
        column = parts[2] #cust, prod, state, etc.

        return agg_func, group_var, column


def generate(input_file=None, *, phi_text=None, phi_params=None):
    """
    Compile phi parameters into MF query-processing Python source.

    Provide exactly one of:
      - input_file: path to a phi-parameter file
      - phi_text: raw phi-parameter text
      - phi_params: already-parsed phi dict

    :returns: generated Python source code (does not write files)
    :rtype: str
    """
    provided = sum(x is not None for x in (input_file, phi_text, phi_params))
    if provided != 1:
        raise ValueError("Provide exactly one of input_file, phi_text, or phi_params")

    if input_file is not None:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file does not exist: {input_file}")
        params = Generator.read_input_to_phi(input_file)
    elif phi_text is not None:
        params = Generator.parse_phi_text(phi_text)
    else:
        params = phi_params

    return Generator.generate_full_program(params)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python3 engine.py <input_file> [output_path]")
        sys.exit(1)

    program = generate(sys.argv[1])
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    written = HelperFunctions.write_to_file(program, output_path)
    print(f"Output written to: {written}")