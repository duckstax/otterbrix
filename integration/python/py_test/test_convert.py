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
    assert to_aggregate(example) == '$aggregate: {$match: {"size": {$eq: "medium"}}}'

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
    assert to_aggregate(example) == '$aggregate: {$match: {$and: [{"size": {$eq: "medium"}}, {"count": {$lt: 10}}, ' \
                                    '{"name": {$regex: "N*"}}]}} '
