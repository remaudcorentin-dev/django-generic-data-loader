
import csv


def csv_to_dict_list(file_path, char_sep="|"):
    """
    Simple function to return a list of python dict from a csv file ('|' as separator)
    """
    with open(file_path, mode='r') as f:
        d = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True, delimiter=char_sep)]
    return d
