import pytest
from ottergon import Client

client = Client()
client["schema"]["table"] # todo: create collection


def gen_id(num):
    res = str(num)
    while (len(res) < 24):
        res = '0' + res
    return res


def test_collection_sql():

    # insert

    query = "INSERT INTO schema.table (_id, name, count) VALUES "
    for num in  range(0, 100):
        query += "('" + gen_id(num + 1) + "', 'Name " + str(num) + "', " + str(num) + ")"
        if num == 99:
            query += ";"
        else:
            query += ", "
    res = client.execute(query)
    assert res.inserted_count == 100

    # select

    res = client.execute("SELECT * FROM schema.table;")
    assert len(res.cursor) == 100
    res.cursor.close()

    # delete

    res = client.execute("SELECT * FROM schema.table WHERE count > 90;")
    assert len(res.cursor) == 9
    res.cursor.close()

    res = client.execute("DELETE FROM schema.table WHERE count > 90;")
    assert res.deleted_count == 9

    res = client.execute("SELECT * FROM schema.table WHERE count > 90;")
    assert len(res.cursor) == 0
    res.cursor.close()

    res = client.execute("SELECT * FROM schema.table;")
    assert len(res.cursor) == 91
    res.cursor.close()

    # update

    res = client.execute("SELECT * FROM schema.table WHERE count < 20;")
    assert len(res.cursor) == 20
    res.cursor.close()

    res = client.execute("SELECT * FROM schema.table WHERE count = 1000;")
    assert len(res.cursor) == 0
    res.cursor.close()

    res = client.execute("UPDATE schema.table SET count = 1000 WHERE count < 20;")
    assert res.modified_count == 20

    res = client.execute("SELECT * FROM schema.table WHERE count < 20;")
    assert len(res.cursor) == 0
    res.cursor.close()

    res = client.execute("SELECT * FROM schema.table WHERE count = 1000;")
    assert len(res.cursor) == 20
    res.cursor.close()
