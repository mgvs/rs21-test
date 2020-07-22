import datetime
from json import dumps

from bson import ObjectId


def default(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, set):
        return list(obj)
    return dumps(obj)


def json_dumps(*args, **kwargs):
    return dumps(default=default, *args, **kwargs)