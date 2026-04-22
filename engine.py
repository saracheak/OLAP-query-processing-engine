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

def read_input(input_file):
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
                phi_params[param].extend([p.strip() for p in parts if p.strip()])
            else:
                phi_params[param].append(line)
    return phi_params


if __name__ == "__main__":
    print(read_input("example_input.txt"))