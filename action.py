# file: action.py
from persistence import repo, Activity
import sys
import printdb


def parse_actions(actions):
    """
    receives the actions file and parse it in order to insert action table
    :param actions: the actions.txt file
    :return: data parsed
    """
    with open(actions, 'r') as parse:
        line: str = parse.readline()
        while line:
            fields = line.replace('\n', "").split(",")
            product = repo.Products.find(int(fields[0]))
            quant = int(fields[1])
            if quant < 0:
                if product.quantity >= -quant:
                    product.quantity = product.quantity + quant
                    repo.Products.update_quantity(product)
                    activity = Activity(*fields)
                    repo.Activities.insert(activity)
            elif quant > 0:
                product.quantity = product.quantity + quant
                repo.Products.update_quantity(product)
                activity = Activity(*fields)
                repo.Activities.insert(activity)

            line: str = parse.readline()


def main(args):
    actions = args[1]

    parse_actions(actions)
    printdb.main()


if __name__ == '__main__':
    # first connect the repo to the db
    repo.init_connection()
    main(sys.argv)
