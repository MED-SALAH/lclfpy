from collections import namedtuple


def toNametuple(className, dict_data):
    return namedtuple(
        className, dict_data.keys()
    )(*tuple(map(lambda x: x if not isinstance(x, dict) else toNametuple(x), dict_data.values())))

