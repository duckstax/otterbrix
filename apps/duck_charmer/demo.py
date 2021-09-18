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
    # todo: add object to the db
    friedrich_collection.insert_one(new_obj)


c = friedrich_collection.find({})
count = 0
for doc in c:
    count += 1

assert count == 100
assert c.count() == 100
assert friedrich_database['FriedrichCollection'].count() == 100


c = friedrich_database['FriedrichCollection']

# assert True for successful drop
assert c.drop() is True

# assert False because collection does not exist anymore
assert c.drop() is False

c = friedrich_database['FriedrichCollection'].find(filter={})
assert c.count() == 100


