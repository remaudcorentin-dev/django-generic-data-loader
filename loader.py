from django.db import transaction
from datetime import datetime
from .utils import plog
from .utils import transform_data

CHUNKS_SIZE = 2000
PRINT_COEF = 10000


def split_existing_and_to_create_records(data, ModelToUpdate, pk):
    """
    Split the 'data' dataset into two lists :
    - One containing elements where the 'pk' match elements found in the database,
    - The other containing elements where the 'pk' wasn't found in the database.
    """
    existing_pks = set(ModelToUpdate.objects.all().values_list(pk, flat=True))
    pks_to_be_create = set()

    existing_records = []
    records_to_create = []

    total_length = len(data)
    i = 0
    plog("Splitting %d records" % total_length)
    for row in data:
        if i % PRINT_COEF == 0:
            plog("%d / %d" % (i, total_length))
        i += 1

        row_pk = row[pk]
        # If the record already exists in the database
        if row_pk in existing_pks:
            existing_records.append(row)
        # The the record doesn't exists in the database
        else:
            # If we already found the record
            if row_pk in pks_to_be_create:
                continue
            records_to_create.append(row)
            pks_to_be_create.add(row_pk)

    return existing_records, records_to_create


def create_new_records(records_to_create, ModelToUpdate):
    """
    Insert new elements in the database.

    We split the list of data to create in chunks of 'CHUNKS_SIZE' elements
     to avoid 'bulk_update' to crash when too many records.
    """

    objs_to_be_created = [ModelToUpdate(**record_to_create) for record_to_create in records_to_create]

    plog("Creating %d new records..." % (len(objs_to_be_created)))
    # Split the list of objects to insert in smaller part to avoid 'bulk_update' to "overload".
    chunks_to_create = [objs_to_be_created[i:i + CHUNKS_SIZE] for i in range(0, len(objs_to_be_created), CHUNKS_SIZE)]
    for chunk_to_create in chunks_to_create:
        ModelToUpdate.objects.bulk_create(chunk_to_create)
    plog("Done creating new records")


def filter_records_to_update(existing_records, ModelsToUpdate, pk):
    """
    Will filter records that needs to be updated - excluding those that hasn't changed.
    """

    if not len(existing_records):
        return {}

    # Get all 'pk' from the data set to update
    existing_records_pks = [row[pk] for row in existing_records]
    # Retrieve all matching elements from the database
    existing_records_in_db = ModelsToUpdate.objects.filter(**{"%s__in" % pk: existing_records_pks})
    # Make it a dict where the key of each element is the 'pk' of the object - will allow fast access to objects.
    existing_records_in_db = {getattr(row, pk): row for row in existing_records_in_db}

    records_to_update = []

    keys = list(existing_records[0].keys())

    for existing_record in existing_records:

        # The object matching the record to update, from the database
        obj_to_update = existing_records_in_db[existing_record[pk]]

        # Concat of all fields to check is something is different between the input and the database.
        existing_compare_key = "".join([str(existing_record.get(key)) for key in keys])
        to_insert_compare_key = "".join([str(getattr(obj_to_update, key)) for key in keys])

        # print("[%s\n %s]\n" % (existing_compare_key, to_insert_compare_key))

        # Compare those key to check if the data has changed
        if to_insert_compare_key != existing_compare_key:
            # Extract the objects to update and remove the 'pk' from the data to update (pop).
            records_to_update.append({
                'obj_to_update': obj_to_update,
                'update_values': existing_record
            })
    return records_to_update


@transaction.atomic
def update_records(records_to_update):
    """
    Will update records to be updated.
    """
    total_length = len(records_to_update)
    plog("Updating %s records" % total_length)
    for i, record_to_update in enumerate(records_to_update):
        if i % PRINT_COEF == 0:
            plog("%d / %d" % (i, total_length))

        # The object from the db to update
        obj_to_update = record_to_update['obj_to_update']
        # A dict containing values to update in the object
        update_values = record_to_update['update_values']

        # Set all values to update to the object
        for key, val in update_values.items():
            setattr(obj_to_update, key, val)
        obj_to_update.save()
    plog("Done updating records")


def get_referenced_records(data, ModelToUpdate, pk):
    """
    Return all elements of type 'ModelToUpdate' referenced in the 'data' by its primary key ('pk' parameter)
    """
    pks = [row[pk] for row in data]
    query_dict = {"%s__in" % pk: pks}
    return ModelToUpdate.objects.filter(**query_dict)


def load_data(data, ModelToUpdate, pk="id", return_only_records_in_data=False):
    """
    Main function to call.
    Sequence the data import process.
    """

    plog("# Loading '%s' data" % ModelToUpdate.__name__)

    # Split records to update and to create
    existing_records, records_to_create = split_existing_and_to_create_records(data, ModelToUpdate, pk)

    # Create new records
    create_new_records(records_to_create, ModelToUpdate)

    # Filter against existing records to keep only those to update
    records_to_update = filter_records_to_update(existing_records, ModelToUpdate, pk)

    # Update records to be updated
    update_records(records_to_update)

    if return_only_records_in_data:
        # Return all records of the 'ModelToUpdate' in form of a dict where the key of each element is the 'pk'.
        # Note: if 'return_only_records_in_data' is 'True' : return only records who are referenced within the 'data'.
        return {getattr(elem, pk): elem for elem in get_referenced_records(data, ModelToUpdate, pk)}
    else:
        # Return all records of the 'ModelToUpdate' in form of a dict where the key of each element is the 'pk'.
        return {getattr(elem, pk): elem for elem in ModelToUpdate.objects.all()}

# Advanced usages bellow

def object_import(raw_data, mapping, ModelToUpdate, pk_field):
    """
    To import objects of type 'ModelToImport' from 'raw_data' according to rules define by the 'mapping' where each \
     object is unique by its 'pk_field' attribute.
    """
    return load_data(
        transform_data(raw_data, mapping.items()),
        ModelToUpdate=ModelToUpdate, pk=pk_field)
