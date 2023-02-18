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


def insert(num):
    new_obj = {}
    new_obj['_id'] = gen_id(num)
    new_obj['count'] = num
    new_obj['countStr'] = str(num)
    new_obj['countBool'] = True if num & 1 else False
    friedrich_collection.insert(new_obj)


for num in range(1000):
    insert(num)

friedrich_collection.create_index(['count'], TypeIndex.SINGLE)
friedrich_collection.create_index(['countStr'], TypeIndex.SINGLE)


def test_collection_find_by_one_index():
    c = friedrich_collection.find({'count': {'$eq': 100}})
    assert len(c) == 1
    c.next()
    assert c['count'] == 100
    c.close()

    c = friedrich_collection.find({'count': {'$eq': 1001}})
    assert len(c) == 0
    c.close()

    c = friedrich_collection.find({'count': {'$gte': 100}})
    assert len(c) == 900
    c.close()

    c = friedrich_collection.find({'count': {'$lt': 100}})
    assert len(c) == 100
    c.close()

    c = friedrich_collection.find({'count': {'$and': [{'$lte': 100}, {'$gt': 50}]}})
    assert len(c) == 50
    c.close()


def test_collection_find_by_two_index():
    c = friedrich_collection.find({'$and': [
        {'count': {'$eq': 100}},
        {'countStr': {'$eq': '100'}}
    ]})
    assert len(c) == 1
    c.next()
    assert c['count'] == 100
    c.close()

    c = friedrich_collection.find({'$and': [
        {'count': {'$eq': 100}},
        {'countStr': {'$eq': '101'}}
    ]})
    assert len(c) == 0
    c.close()

    c = friedrich_collection.find({'$or': [
        {'count': {'$eq': 100}},
        {'countStr': {'$eq': '101'}}
    ]})
    assert len(c) == 2
    c.close()


def test_collection_find_by_one_index_after_insert():
    c = friedrich_collection.find({'count': {'$eq': 1001}})
    assert len(c) == 0
    c.close()

    insert(1001)

    c = friedrich_collection.find({'count': {'$eq': 1001}})
    assert len(c) == 1
    c.next()
    assert c['count'] == 1001
    c.close()
