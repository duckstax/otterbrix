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
    #collection.delete_many({})
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
        pass
        #client.close() #todo

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
    #count = 0
    #for doc in cursor:
        #count += 1
    #assert count == 100
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
