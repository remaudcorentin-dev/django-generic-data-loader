# django-generic-data-loader
Django Generic Data Load - For fast data import into Django Models (project in progress)

#### Simple sets of function to create fast and reliable data import.

Very minimalistic usage example bellow

```python

from models import User
from models import UserRole

from .loader import object_import

from .extract import csv_to_dict_list

from .utils import var_to_fk


def mappings(mapping_name, env=None, kwargs=None):
    """
    Defines the mapping between data to import (from files to import) and the database fields,
    Also defines the rules to apply on each data to import.
    A "custom" function can be applied on a specific data.
    """
    return {
        "user_role": {
        	"USER_ROLE": {"name": "name"}
        },
        "user": {
            "USERNAME": {"name": "username"},  # PK
            "FIRST_NAME": {"name": "first_name"},
            "LAST_NAME": {"name": "last_name"},
            "EMAIL": {"name": "email"},
            "USER_ROLE": {"name": "role", "function": var_to_fk, "args": {"query": env['user_roles']}}
        }
    }.get(mapping_name, {})


def run_import(file_path):
	# Extract data from the csv file
	raw_data = csv_to_dict_list(file_path)

	# Initialize the data import environment
	env = {}

	# Create / update and retrieve 'UserRole' objects to / from the database
	env['user_roles'] = object_import(raw_data, mappings("user_role"), UserRole, "name")

	# Create / update and retrieve 'User' objects to / from the database using the previously created 'UserRole' objects
	env['users'] = object_import(raw_data, mappings("user", env), UserRole, "username")

	return env

# Call the function to run the data import of the file in parameter.
run_import("dataset.exemple.csv")

```

TODO:
- Advanced usage example(s),
- Description of all available rules and functions,
- Guide to create additional "standard custom" functions
