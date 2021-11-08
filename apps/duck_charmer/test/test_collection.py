import os
import copy
import json
import pytest
from duck_charmer import Client,DataBase, Collection

client = Client()
friedrich_database = client["FriedrichDatabase"]
friedrich_collection = friedrich_database["FriedrichCollection"]

for num in range(100):
    new_obj = {}
    new_obj['_id'] = str(num)
    new_obj['count'] = num
    new_obj['countStr'] = str(num)
    new_obj['countFloat'] = float(num) + 0.1
    new_obj['countBool'] = True if num & 1 else False
    new_obj['countArray'] = [num + i for i in range(5)]
    new_obj['countDict'] = {
        'odd': bool(num & 1),
        'even': not (num & 1),
        'three': not (num % 3),
        'five': not (num % 5),
    }
    new_obj['nestedArray'] = [[num + i] for i in range(5)]
    new_obj['dictArray'] = [{'number': num + i} for i in range(5)]
    new_obj['mixedDict'] = copy.deepcopy(new_obj)
    friedrich_collection.insert(new_obj)


def test_collection_len():
    assert len(friedrich_collection) == 100
    assert len(friedrich_database['FriedrichCollection']) == 100


def test_collection_find():
    c = friedrich_collection.find({})
    assert len(c) == 100

    while (c.next()):
        assert str(c['count']) == c['countStr']
    c.close()

    #c = friedrich_collection.find({'count': {'$gt': 90}})
    #assert len(c) == 9
    #c.close()

    #c = friedrich_collection.find({'countStr': {'$regex': '.*9'}})
    #assert len(c) == 10
    #c.close()

    #c = friedrich_collection.find({'$or': [{'count': {'$gt': 90}}, {'countStr': {'$regex': '.*9'}}]})
    #assert len(c) == 18
    #c.close()

    #c = friedrich_collection.find({'$and': [{'$or': [{'count': {'$gt': 90}}, {'countStr': {'$regex': '.*9'}}]}, {'count': {'$lte': 30}}]})
    #assert len(c) == 3
    #c.close()

test_collection_find()

#c = friedrich_database['FriedrichCollection']

#assert True for successful drop
#assert c.drop() is True

#assert False because collection does not exist anymore
#assert c.drop() is False

#c = friedrich_database['FriedrichCollection'].find(filter={})
#assert c.count() == 0
