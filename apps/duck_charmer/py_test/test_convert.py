#import pytest
import time
from duck_charmer import to_statement


def test_convector():
    dir(duck_charmer)
    help(duck_charmer)
    example = [

        {
            "$match": {
                "size": "medium"
            }
        },
        {
            "$group": {
                "_id": "$name",
                "totalQuantity": {"$sum": "$quantity"}
            }
        }
    ]
    to_statement(example)
