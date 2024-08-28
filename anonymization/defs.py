from enum import Enum
import datetime
import yaml
from collections import namedtuple


class ColumnType(Enum):
    STRING = 0
    FLOAT = 1
    INTEGER = 2
    DATE = 3
    CATEGORICAL = 4


Column = namedtuple('Column', ['name', 'type', 'impute', 'impute_value', 'keep_null', 'lower', 'upper'])

global dateformat


def date2timestamp(s):
    return datetime.datetime.strptime(s, dateformat).timestamp()


def timestamp2date(ts):
    if ts != ts:
        return None
    return str(datetime.date.fromtimestamp(ts))


def load_yaml(path):
    with open(path) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def get_dp_query_str(q_type, column_name, lower, upper):
    # return q_type+"("+column_name+", lower="+lower+", upper="+upper+")"
    return "{}({}, lower={}, upper={})".format(q_type, column_name, lower, upper)
