import pytest
from ottergon import to_aggregate


def test_convector():
    example = [
        {
            "$match": {
                "size": "medium"
            }
        }
    ]
    assert to_aggregate(example) == '$aggregate: {$match: {"size": {$eq: #0}}}'

    example = [
        {
            "$match": {
                "size": "medium",
                "count": {
                    "$lt": 10
                },
                "name": {
                    "$regex": "N*"
                }
            }
        }
    ]
    assert to_aggregate(example) == '$aggregate: {$match: {$and: [{"size": {$eq: #0}}, {"count": {$lt: #1}}, ' \
                                    '{"name": {$regex: #2}}]}}'
