
from datetime import datetime

import unicodedata
import os
import re


def plog(s):
    os.system("echo '[%s] :' '%s' >> /tmp/etl_data_import.log" % (datetime.now(), s))
    print(s)


def var_to_fk(row, key, query):
    query_key = row.get(key, "")
    return query[query_key].id if query_key in query else None


def var_to_fk_none_if_key_is_blank(row, key, query):
    query_key = row.get(key, "")
    if not query_key:
        return None
    return var_to_fk(row, key, query)


def run_row_mapping(row_in, mapping):
    row_out = {}
    for key, val in mapping:
        args = val['args'] if 'args' in val else {}
        row_out[val['name']] = val['function'](row_in, key, **args) if 'function' in val and val['function'] else row_in.get(key, "")
    return row_out


def transform_data(data_in, mapping):
    data_out = []
    for row in data_in:
        data_out.append(run_row_mapping(row, mapping))
    return data_out


def str_to_sa(s):
    """
    Remove/Replace all 'weird' chars from the string in parameter according to dict bellow.
    """
    try:
        tab = {"Á": "A", "À": "A", "Ă": "A", "Â": "A", "Å": "A", "Ä": "A", "Ã": "A", "Ą": "A", "Æ": "AE", "Ć": "C",
               "Č": "C", "Ç": "C", "Ď": "D", "Đ": "D", "É": "E", "È": "E", "Ê": "E", "Ě": "E", "Ë": "E", "Ę": "E",
               "Ğ": "G", "Í": "I", "Ì": "I", "Î": "I", "Ï": "I", "Ĺ": "L", "Ľ": "L", "Ł": "L", "Ń": "N", "Ň": "N",
               "Ñ": "N", "Ó": "O", "Ò": "O", "Ô": "O", "Ö": "O", "Õ": "O", "Ø": "O", "Œ": "OE", "Ŕ": "R", "Ř": "R",
               "Ś": "S", "Š": "S", "Ş": "S", "Ș": "S", "Ť": "T", "Ț": "T", "Ú": "U", "Ù": "U", "Û": "U", "Ü": "U",
               "Ý": "Y", "Ÿ": "Y", "Ź": "Z", "Ž": "Z", "Ż": "Z", "Þ": "T", "'": "", "’": "", "‘": "", '“': "", '”': "",
               '"': "",  "ø": "o"}
        pattern = re.compile('|'.join(tab.keys()))
        res = pattern.sub(lambda x: tab[x.group()], s)
        return " ".join(res.split())
    except Exception as e:
        return ""


def normalize_str(s):
    try:
        return str_to_sa("".join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')))
    except:
        return ""


def boolean_convert(row, key):
    val = row.get(key, None)
    if val == "1" or val == 1 or \
       (type(val) == str and val.upper() == "TRUE") or \
       (type(val) == str and val.upper() == "YES"):
        return True
    return False


def concat_values(row, _, keys):
    return "".join([row.get(key, "") for key in keys])


def constant_value(row, _, value):
    return value


def datetime_from_str(str_datetime, dt_format):
    return datetime.strptime(str_datetime, dt_format)


def date_from_str_datetime(row, _, input_field_name, input_datetime_format):
    str_dt = row.get(input_field_name, None)
    if not str_dt:
        return None
    return datetime_from_str(input_field_name, input_datetime_format).date()


def time_from_str_datetime(row, _, input_field_name, input_datetime_format):
    str_dt = row.get(input_field_name, None)
    if not str_dt:
        return None
    return datetime_from_str(input_field_name, input_datetime_format).time()
