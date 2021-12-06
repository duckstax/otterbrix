import copy
import pytest
from duck_charmer import Client, DataBase, Collection

database_name = "TestDatabase"
collection_name = "TestCollection"

client = Client() #todo host:port
database = client[database_name]
collection = database[collection_name]


@pytest.fixture()
def gen_collection(request):
    collection = database[collection_name]
    for num in range(100):
        obj = {}
        obj['_id'] = str(num) #delete
        obj['count'] = num
        obj['countStr'] = str(num)
        obj['countFloat'] = float(num) + 0.1
        obj['countBool'] = True if num & 1 else False
        obj['countArray'] = [num + i for i in range(5)]
        obj['countDict'] = {
            'odd': bool(num & 1),
            'even': not(num & 1),
            'three': not(num % 3),
            'five': not(num % 5),
        }
        obj['nestedArray'] = [[num + i] for i in range(5)]
        obj['dictArray'] = [{'number': num + i} for i in range(5)]
        obj['mixedDict'] = copy.deepcopy(obj)
        collection.insert_one(obj)

    def finalize():
        collection.drop()

    request.addfinalizer(finalizer=finalize)

    return {
        'collection': collection
    }


#todo
#def test_access_database_by_subscription():
#    assert client[database_name] == database


#todo
#def test_access_collection_by_subscription():
#    assert isinstance(database[collection_name], collection)


def test_list_collections():
    tables = database.collection_names()
    #assert '_default' in tables
    assert collection_name in tables


def test_initialize_collection(gen_collection):
    c = gen_collection['collection']
    cursor = c.find({})
    count = 0
    for doc in cursor:
        count += 1
    assert count == 100
    assert cursor.count() == 100
    assert len(cursor) == 100
    assert c.count() == 100
    assert len(c) == 100
    assert len(c.find({})) == 100


def test_drop_collection(gen_collection):
    c = gen_collection['collection']
    assert database.drop_collection(collection_name) is True
    assert database.drop_collection(collection_name) is False
    c = database[collection_name]
    assert c.drop() is True
    assert c.drop() is False


def test_find_with_filter_named_parameter(gen_collection):
    c = gen_collection['collection'].find(filter={})
    assert c.count() == 100
    assert gen_collection['collection'].find(filter={}).count() == 100


def test_greater_than(gen_collection):
    c = gen_collection['collection'].find({'count': {'$gte': 50}})
    assert c.count() == 50
    assert gen_collection['collection'].find({'count': {'$gte': 50}}).count() == 50


def test_find_in_subdocument(gen_collection):
    c = gen_collection['collection'].find({'mixedDict.count': 0})
    assert c.count() == 1
    assert gen_collection['collection'].find({'mixedDict.count': 0}).count() == 1


def test_find_in_subdocument_with_operator(gen_collection):
    c = gen_collection['collection'].find({'mixedDict.count': {'$gte': 50}})
    assert c.count() == 50
    assert gen_collection['collection'].find({'mixedDict.count': {'$gte': 50}}).count() == 50


def test_find_in_subdocument_3_levels(gen_collection):
    c = gen_collection['collection'].find({'mixedDict.countDict.even': True})
    assert c.count() == 50
    assert gen_collection['collection'].find({'mixedDict.countDict.even': True}).count() == 50


def test_find_in_subdocument_with_array(gen_collection):
    c = gen_collection['collection'].find({'mixedDict.countArray.3': {'$gt': 50}})
    assert c.count() == 52
    assert gen_collection['collection'].find({'mixedDict.countArray.3': {"$gt": 50}}).count() == 52


def test_delete_one(gen_collection):
    result = gen_collection['collection'].delete_one({'countBool': True})
    assert result.deleted_count == 1
    c = gen_collection['collection'].find({})
    assert c.count() == 99
    result = gen_collection['collection'].delete_one({'countBool': True})
    assert result.deleted_count == 1
    c = gen_collection['collection'].find({})
    assert c.count() == 98


def test_delete_all(gen_collection):
    result = gen_collection['collection'].delete_many({})
    assert result.deleted_count == 100
    c = gen_collection['collection'].find({})
    assert c.count() == 0


def test_delete_many(gen_collection):
    result = gen_collection['collection'].delete_many({'count': {'$gte': 50}})
    assert result.deleted_count == 50
    c = gen_collection['collection'].find({})
    assert c.count() == 50


def test_update_one(gen_collection):
    result = gen_collection['collection'].update_one({'count': {'$eq': 50}}, {'$set': {'countStr': '500'}})
    assert result.modified_count == 1
    c = gen_collection['collection'].find({'count': {'$eq': 50}})
    c.next()
    assert c['countStr'] == '500'


def test_update_many(gen_collection):
    result = gen_collection['collection'].update_many({'count': {'$gte': 50}}, {'$inc': {'count': 100}})
    assert result.modified_count == 50
    assert gen_collection['collection'].find({'count': {'$gt': 100}}).count() == 50


def test_update_with_add_new_field(gen_collection):
    assert gen_collection['collection'].find({'countStr2': {'$eq': '500'}}).count() == 0
    result = gen_collection['collection'].update_one({'count': {'$eq': 50}}, {'$set': {'countStr2': '500'}})
    assert result.modified_count == 1
    assert gen_collection['collection'].find({'countStr2': {'$eq': '500'}}).count() == 1


def test_update_with_upsert(gen_collection):
    result = gen_collection['collection'].update_one({'count': {'$eq': 100}}, {'$set': {'countStr': '500'}})
    assert result.modified_count == 0
    assert result.matched_count == 0
    assert len(result.upserted_id) == 0
    assert gen_collection['collection'].find({'countStr': {'$eq': '500'}}).count() == 0

    result = gen_collection['collection'].update_one({'count': {'$eq': 100}}, {'$set': {'countStr': '500'}}, True)
    assert result.modified_count == 0
    assert result.matched_count == 0
    assert len(result.upserted_id) > 0
    assert gen_collection['collection'].find({'countStr': {'$eq': '500'}}).count() == 1
