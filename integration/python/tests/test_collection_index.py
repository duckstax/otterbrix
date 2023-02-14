import pytest
from ottergon import Client, DataBase, Collection, TypeIndex


def gen_id(num):
    res = str(num)
    while (len(res) < 24):
        res = '0' + res
    return res


client = Client()
friedrich_database = client["FriedrichDatabase"]
friedrich_collection = friedrich_database["FriedrichCollection"]

for num in range(1000):
    new_obj = {}
    new_obj['_id'] = gen_id(num)
    new_obj['count'] = num
    new_obj['countStr'] = str(num)
    new_obj['countBool'] = True if num & 1 else False
    friedrich_collection.insert(new_obj)
    friedrich_collection.create_index(['count'], TypeIndex.SINGLE)


def test_collection_find():
    c = friedrich_collection.find({})
    assert len(c) == 1000
    c.close()

    # c = friedrich_collection.find({'count': {'$gt': 900}})
    # assert len(c) == 9
    # c.close()
    #
    # c = friedrich_collection.find({'countStr': {'$regex': '9$'}})
    # assert len(c) == 10
    # c.close()
    #
    # c = friedrich_collection.find({'$or': [{'count': {'$gt': 90}}, {'countStr': {'$regex': '9$'}}]})
    # assert len(c) == 18
    # c.close()
    #
    # c = friedrich_collection.find({'$and': [{'$or': [{'count': {'$gt': 90}}, {'countStr': {'$regex': '9$'}}]}, {'count': {'$lte': 30}}]})
    # assert len(c) == 3
    # c.close()
