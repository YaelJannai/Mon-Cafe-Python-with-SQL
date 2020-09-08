# file: printdb.py
from persistence import repo


def print_list(data):
    """
    receive some data, run over it line by line and print it, by making the received object a tuple
    :param data: what to print
    :return: the line printed to screen
    """
    for elem in data:
        print(elem.to_tuple())


def print_employees():
    """
    print employees one by one, calls print_list function
    :return: data printed
    """
    print("Employees")
    data = repo.Employees.find_all()
    print_list(data)


def print_suppliers():
    """
    print suppliers one by one, calls print_list function
    :return: data printed
    """
    print("Suppliers")
    data = repo.Suppliers.find_all()
    print_list(data)


def print_products():
    """
    print products one by one, calls print_list function
    :return: data printed
    """
    print("Products")
    data = repo.Products.find_all()
    print_list(data)


def print_coffee_stands():
    """
    print coffee_stands one by one, calls print_list function
    :return: data printed
    """
    print("Coffee stands")
    data = repo.Coffee_stands.find_all()
    print_list(data)


def print_activities():
    """
    print activities one by one, calls print_list function
    :return: data printed
    """
    print("Activities")
    data = repo.Activities.find_all()
    print_list(data)


def print_employees_report():
    """
    print employees_report, calculate the employee's income with another function
    :return: data printed
    """
    print("Employees report")
    data = repo.find_all_employee_report()

    for rep in data:
        income = repo.products_price_total_by_activator(rep.id)
        rep.total_sales_income = _calculate_income(income)
        print(rep)


def _calculate_income(arr_tup):
    """
    calculates the income by number of sale items and their cost
    :param arr_tup: the tuple's array contain the data to calculate with
    :return: the sum of income
    """
    _sum = 0
    for elem in arr_tup:
        _sum = _sum + abs(elem[0]*elem[1])
    return _sum


def print_activities_report():
    """
    print activities report - all activities that happened
    :return: data printed
    """
    print("Activities")
    data = repo.find_all_activity_report()
    print_list(data)


def main():
    print_activities()
    print_coffee_stands()
    print_employees()
    print_products()
    print_suppliers()
    print('')
    print_employees_report()
    if repo.activities_exists():
        print('')
        print_activities_report()


if __name__ == '__main__':
    # first connect the repo to the db
    repo.init_connection()
    main()
