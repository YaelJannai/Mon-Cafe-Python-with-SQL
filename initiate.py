# file: initiate.py

from persistence import repo, Employee, Supplier, Product, Coffee_stand
import sys
import printdb
import os


def parse_config(content):
    """
    receives the configuration file and parse it in order to insert all the tables, calls the DTO's constructors
    :param content: the configuration.txt file
    :return: data parsed
    """
    with open(content, 'r') as parse:
        line: str = parse.readline()
        while line:
            fields = line.replace('\n', "").split(", ")
            if fields[0] == 'E':
                employee = Employee(*fields[1:])
                repo.Employees.insert(employee)
            elif fields[0] == 'S':
                supplier = Supplier(*fields[1:])
                repo.Suppliers.insert(supplier)
            elif fields[0] == 'P':
                if len(fields) == 4:
                    fields.append('0')
                product = Product(*fields[1:])
                repo.Products.insert(product)
            elif fields[0] == 'C':
                coffee_stand = Coffee_stand(*fields[1:])
                repo.Coffee_stands.insert(coffee_stand)
            line: str = parse.readline()


def main(args):
    content = args[1]
    repo.create_tables()
    parse_config(content)


if __name__ == '__main__':
    # first if there is a data base with that name remove it
    if os.path.exists("moncafe.db"):
        os.remove("moncafe.db")
    # initiate the connection of the repo
    repo.init_connection()
    main(sys.argv)
