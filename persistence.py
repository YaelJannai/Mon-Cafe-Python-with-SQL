# file: persistence.py
import sqlite3
import atexit


# Data Transfer Objects:

class Employee(object):
    def __init__(self, id_number, name, salary, coffee_stand):
        """
        an Employee DTO object
        :param id_number: the id of the employee
        :param name:  the name of the employee
        :param salary: the salary of the employee
        :param coffee_stand: the stand that the employee work in
        """
        self.id = id_number
        self.name = name
        self.salary = salary
        self.coffee_stand = coffee_stand

    def to_tuple(self):
        """
        receives an object and make it a tuple of all object data
        :return: tuple from object
        """
        tup = (self.id, self.name, self.salary, self.coffee_stand)
        return tup


class Supplier(object):
    def __init__(self, id_number, name, contact_information):
        """
        a Supplier DTO object
        :param id_number: the id of the supplier
        :param name: the name of the supplier
        :param contact_information: the information of the contract with the supplier
        """
        self.id = id_number
        self.name = name
        self.contact_information = contact_information

    def to_tuple(self):
        """
        receives an object and make it a tuple of all object data
        :return: tuple from object
        """
        tup = (self.id, self.name, self.contact_information)
        return tup


class Product(object):
    def __init__(self, id_number, description, price, quantity):
        """
        a Product DTO object
        :param id_number: the id of the product
        :param description: the name of the product
        :param price: the price of the product
        :param quantity: the quantity of the product
        """
        self.id = id_number
        self.description = description
        self.price = price
        self.quantity = quantity

    def to_tuple(self):
        """
        receives an object and make it a tuple of all object data
        :return: tuple from object
        """
        tup = (self.id, self.description, self.price, self.quantity)
        return tup


class Coffee_stand(object):
    def __init__(self, id_number, location, number_of_employees):
        """
        a Coffee stand DTO object
        :param id_number: the id of the coffee stand
        :param location: the location of the coffee stand
        :param number_of_employees: the number of employees that work in the coffee stand
        """
        self.id = id_number
        self.location = location
        self.number_of_employees = number_of_employees

    def to_tuple(self):
        """
        receives an object and make it a tuple of all object data
        :return: tuple from object
        """
        tup = (self.id, self.location, self.number_of_employees)
        return tup


class Activity(object):
    def __init__(self, product_id, quantity, activator_id, date):
        """
        an Activity DTO object
        :param product_id: the id of the product which the activity was made on
        :param quantity: the quantity of change of the product
        :param activator_id: who did the action
        :param date: the date which the activity happened
        """
        self.product_id = product_id
        self.quantity = quantity
        self.activator_id = activator_id
        self.date = date

    def to_tuple(self):
        """
        receives an object and make it a tuple of all object data
        :return: tuple from object
        """
        tup = (self.product_id, self.quantity, self.activator_id, self.date)
        return tup


class Employee_report(object):
    def __init__(self, id_number, name, salary, working_location):
        """
        an Employee report object - serve for printing a joined tables
        :param id_number: the id of the employee
        :param name: the name of the employee
        :param salary: the salary of the employee
        :param working_location: where the employee works
        """
        self.id = id_number
        self.name = name
        self.salary = salary
        self.working_location = working_location
        self.total_sales_income = 0

    def __str__(self):
        return self.name + " " + str(self.salary) + " " + self.working_location + " " + str(self.total_sales_income)


class Activity_report(object):
    def __init__(self, date, product_name, quantity, seller_name, supplier_name):
        """
        an Activity report object - serve for printing a joined tables
        :param date: the date that the activity happened
        :param product_name: the name of the product that was changed
        :param quantity: the quantity of change of the product
        :param seller_name: the name of the seller - employee (could be none)
        :param supplier_name: the name of the supplier (could be none)
        """
        self.date = date
        self.product_name = product_name
        self.quantity = quantity
        self.seller_name = seller_name
        self.supplier_name = supplier_name

    def to_tuple(self):
        """
        receives an object and make it a tuple of all object data
        :return: tuple from object
        """
        tup = (self.date, self.product_name, self.quantity, self.seller_name, self.supplier_name)
        return tup


# Data Access Objects:
# All of these are meant to be singletons
class _Employees:
    def __init__(self, conn):
        """
        the ctor of the class that represents the Employees table - DAO
        :param conn: connection to the database
        """
        self._conn = conn

    def insert(self, one_Employee):
        """
        insert new employee to the table
        :param one_Employee: an employee DTO object to be inserted to the table
        """
        self._conn.execute("""
               INSERT INTO Employees (id, name, salary, coffee_stand) VALUES (?, ?, ?, ?)
           """, [one_Employee.id, one_Employee.name, one_Employee.salary, one_Employee.coffee_stand])

    def find(self, Employee_id):
        """
        find a record of an employee by the given id
        :param Employee_id: the id of the employee to be found
        :return: a DTO Employee object with the parameters of the record in the table
        """
        c = self._conn.cursor()
        c.execute("""
            SELECT id, name, salary, coffee_stand FROM Employees WHERE id = ?
        """, [Employee_id])

        return Employee(*c.fetchone())

    def find_all(self):
        """
        find all the employees in the table
        :return: a list of tuples of employees parameters
        """
        c = self._conn.cursor()
        _all =  c.execute("""
            SELECT * FROM Employees ORDER BY Employees.id ASC
        """).fetchall()

        return [Employee(*row) for row in _all]


class _Suppliers:
    def __init__(self, conn):
        """
        the ctor of the class that represents the Suppliers table - DAO
        :param conn: connection to the database
        """
        self._conn = conn

    def insert(self, one_Supplier):
        """
        insert new supplier to the table
        :param one_Supplier: a Supplier DTO object to be inserted to the table
        """
        self._conn.execute("""
               INSERT INTO Suppliers (id, name, contact_information) VALUES (?, ?, ?)
           """, [one_Supplier.id, one_Supplier.name, one_Supplier.contact_information])

    def find(self, Supplier_id):
        """
        find a record of a supplier by the given id
        :param Supplier_id: the id of the supplier to be found
        :return: a DTO Supplier object with the parameters of the record in the table
        """
        c = self._conn.cursor()
        c.execute("""
            SELECT id, name, contact_information FROM Suppliers WHERE id = ?
        """, [Supplier_id])

        return Supplier(*c.fetchone())

    def find_all(self):
        """
        find all the suppliers in the table
        :return: a list of tuples of suppliers parameters
        """
        c = self._conn.cursor()
        _all = c.execute("""
            SELECT * FROM Suppliers ORDER BY Suppliers.id ASC
        """).fetchall()

        return [Supplier(*row) for row in _all]


class _Products:
    def __init__(self, conn):
        """
        the ctor of the class that represents the Products table - DAO
        :param conn: connection to the database
        """
        self._conn = conn

    def insert(self, one_Product):
        """
        insert new product to the table
        :param one_Product: a Product DTO object to be inserted to the table
        """
        self._conn.execute("""
               INSERT INTO Products (id, description, price, quantity) VALUES (?, ?, ?, ?)
           """, [one_Product.id, one_Product.description, one_Product.price, one_Product.quantity])

    def find(self, Product_id):
        """
        find a record of a product by the given id
        :param Product_id: the id of the product to be found
        :return: a DTO Product object with the parameters of the record in the table
        """
        c = self._conn.cursor()
        c.execute("""
            SELECT id, description, price, quantity FROM Products WHERE id = ?
        """, [Product_id])

        return Product(*c.fetchone())

    def update_quantity(self, one_Product):
        """
        updating the quantity of a product
        :param one_Product: the DTO object with the updated quantity already
        """
        self._conn.execute("""
                   Update Products Set quantity = ? Where id = ?
               """, [one_Product.quantity, one_Product.id])

    def find_all(self):
        """
        find all the products in the table
        :return: a list of tuples of products parameters
        """
        c = self._conn.cursor()
        _all = c.execute("""
            SELECT * FROM Products ORDER  BY Products.id ASC
        """).fetchall()

        return [Product(*row) for row in _all]


class _Coffee_stands:
    def __init__(self, conn):
        """
        the ctor of the class that represents the Coffee stands table - DAO
        :param conn: connection to the database
        """
        self._conn = conn

    def insert(self, one_Coffee_stand):
        """
        insert new coffee stand to the table
        :param one_Coffee_stand: a Coffee_stand DTO object to be inserted to the table
        """
        self._conn.execute("""
               INSERT INTO Coffee_stands (id, location, number_of_employees) VALUES (?, ?, ?)
           """, [one_Coffee_stand.id, one_Coffee_stand.location, one_Coffee_stand.number_of_employees])

    def find(self, Coffee_stand_id):
        """
         find a record of a coffee stand by the given id
         :param Coffee_stand_id: the id of the coffee stand to be found
         :return: a DTO Coffee_stand object with the parameters of the record in the table
         """
        c = self._conn.cursor()
        c.execute("""
            SELECT id, location, number_of_employees FROM Coffee_stands WHERE id = ?
        """, [Coffee_stand_id])

        return Coffee_stand(*c.fetchone())

    def find_all(self):
        """
        find all the coffee stands in the table
        :return: a list of coffee stands of suppliers parameters
        """
        c = self._conn.cursor()
        _all = c.execute("""
            SELECT * FROM Coffee_stands ORDER  BY Coffee_stands.id ASC
        """).fetchall()

        return [Coffee_stand(*row) for row in _all]


class _Activities:
    def __init__(self, conn):
        """
        the ctor of the class that represents the Activities table - DAO
        :param conn: connection to the database
        """
        self._conn = conn

    def insert(self, one_Activity):
        """
        insert new activity to the table
        :param one_Activity: a Activity DTO object to be inserted to the table
        """
        self._conn.execute("""
               INSERT INTO Activities (product_id, quantity, activator_id, date) VALUES (?, ?, ?, ?)
           """, [one_Activity.product_id, one_Activity.quantity, one_Activity.activator_id, one_Activity.date])

    def find_all(self):
        """
        find all the activities in the table
        :return: a list of tuples of activities parameters
        """
        c = self._conn.cursor()
        _all = c.execute("""
            SELECT * FROM Activities ORDER  BY Activities.date ASC
        """).fetchall()

        return [Activity(*row) for row in _all]


# The Repository
class _Repository(object):
    def __init__(self):
        """
        the ctor of the Repository - singleton
        initiate the connection and creating the singletons that represents the tables
        """
        self._conn = None
        self.Employees = None
        self.Suppliers = None
        self.Products = None
        self.Coffee_stands = None
        self.Activities = None

    def init_connection(self):
        """
        initiate the connection with the database
        """
        self._conn = sqlite3.connect('moncafe.db')
        self.Employees = _Employees(self._conn)
        self.Suppliers = _Suppliers(self._conn)
        self.Products = _Products(self._conn)
        self.Coffee_stands = _Coffee_stands(self._conn)
        self.Activities = _Activities(self._conn)

    def close(self):
        """
        closing the connection to the database
        """
        self._conn.commit()
        self._conn.close()

    def create_tables(self):
        """
        this method is in charge to create the tables in the database
        """

        # executing the creation of the tables: Employees, Suppliers, Products, Coffee_stands, Activities
        self._conn.cursor().executescript("""
                CREATE TABLE Employees (
                    id              INTEGER     PRIMARY KEY,
                    name            TEXT        NOT NULL,
                    salary          REAL        NOT NULL,
                    coffee_stand    INTEGER     REFERENCES Coffee_stand(id)
                );

                CREATE TABLE Suppliers (
                    id                  INTEGER     PRIMARY KEY,
                    name                TEXT        NOT NULL,
                    contact_information TEXT
                );

                CREATE TABLE Products (
                    id      INTEGER     PRIMARY KEY,
                    description  TEXT     NOT NULL,
                    price           REAL     NOT NULL,
                    quantity    INTEGER     NOT NULL
                );

                CREATE TABLE Coffee_stands (
                    id      INTEGER     PRIMARY KEY,
                    location  TEXT     NOT NULL,
                    number_of_employees    INTEGER
                );

                CREATE TABLE Activities (
                    product_id      INTEGER     INTEGER REFERENCES Product(id),
                    quantity  INTEGER     NOT NULL,
                    activator_id    INTEGER NOT NULL,
                    date DATE NOT NULL
                );
            """)

    def find_all_employee_report(self):
        """
        a join method that join between the employees and location of the coffee stands they work in
        :return: a list of DTO Employee_report objects
        """
        c = self._conn.cursor()
        _all = c.execute("""
            SELECT Employees.id, Employees.name, Employees.salary, Coffee_stands.location 
            FROM Coffee_stands
            JOIN Employees ON Coffee_stands.id = employees.coffee_stand
            ORDER BY Employees.name ASC
        """).fetchall()
        return [Employee_report(*row) for row in _all]

    def products_price_total_by_activator(self, activator_id):
        """
        this method go over all the activities that where made by the given activator
        and join the quantity of each product with the price of it.
        this method serve to help calculate for each employee (activator) his total sales income.
        :param activator_id: the id of the activator of the action
        :return: list of tuples, tuple[0] - the quantity of the product, tuple[1] - the price of the product
        """
        c = self._conn.cursor()
        return c.execute("""
                   SELECT Activities.quantity, Products.price
                   FROM Activities JOIN Products ON Activities.product_id = Products.id
                   WHERE Activities.activator_id = ?
               """, [activator_id]).fetchall()

    def find_all_activity_report(self):
        """
        a join method that join between the activities, the name of the product,
        the name of the seller and the name of the supplier - could be none
        :return: a list of tuples of activity reports parameters
        """
        c = self._conn.cursor()
        _all = c.execute("""
                   SELECT Activities.date, Products.description, Activities.quantity, Employees.name, Suppliers.name
                   FROM Activities
                   JOIN Products ON Activities.product_id = Products.id
                   LEFT JOIN Employees ON Activities.activator_id = Employees.id
                   LEFT JOIN Suppliers ON Activities.activator_id = Suppliers.id
                   ORDER BY Activities.date ASC
               """).fetchall()
        return [Activity_report(*row) for row in _all]


    def activities_exists(self):
        """
        this method checks if the Activities table was filled already, or is empty
        :return: true - if the table is not empty, false - else
        """
        c = self._conn.cursor()
        c.execute("SELECT date FROM Activities")
        if c.fetchone() is None:
            return False
        else:
            return True


# the repository singleton
repo = _Repository()
atexit.register(repo.close)
