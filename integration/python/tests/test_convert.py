import pytest
import sys
from otterbrix import to_aggregate


def test_convert_aggregate_match():
    try:
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
        assert to_aggregate(example) == '$aggregate: {$match: {$and: ["size": {$eq: #0}, "count": {$lt: #1}, ' \
                                        '"name": {$regex: #2}]}}'
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def test_convert_aggregate_group():
    try:
        example = [
            {
                "$group": {
                    "_id": "name"
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {_id: #0}}'

        example = [
            {
                "$group": {
                    "_id": "$name"
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {_id: "$name"}}'

        example = [
            {
                "$group": {
                    "sum": {
                        "$sum": "$count"
                    }
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {sum: {$sum: "$count"}}}'

        example = [
            {
                "$group": {
                    "total": {
                        "$multiply": [
                            "$price",
                            "$count"
                        ]
                    }
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {total: {$multiply: ["$price", "$count"]}}}'

        example = [
            {
                "$group": {
                    "total": {
                        "$multiply": [
                            "$price",
                            10
                        ]
                    }
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {total: {$multiply: ["$price", #0]}}}'

        example = [
            {
                "$group": {
                    "_id": "$name",
                    "sum": {
                        "$sum": "$count"
                    }
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {_id: "$name", sum: {$sum: "$count"}}}'

        example = [
            {
                "$group": {
                    "_id": "$name",
                    "type": "type",
                    "total": {
                        "$sum": {
                            "$multiply": [
                                "$price",
                                "$count"
                            ]
                        }
                    }
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$group: {_id: "$name", type: #0, total: {$sum: {$multiply: '\
                                        '["$price", "$count"]}}}}'
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def test_convert_aggregate_sort():
    try:
        example = [
            {
                "$sort": {
                    "name": 1
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$sort: {name: 1}}'

        example = [
            {
                "$sort": {
                    "name": 1,
                    "count": -1
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {$sort: {name: 1, count: -1}}'
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise


def test_convert_aggregate():
    try:
        example = [
            {
                "$match": {
                    "size": "medium"
                }
            },
            {
                "$group": {
                    "total": {
                        "$multiply": [
                            "$price",
                            10
                        ]
                    }
                }
            },
            {
                "$sort": {
                    "name": 1
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {'\
                                        '$match: {"size": {$eq: #0}}, '\
                                        '$group: {total: {$multiply: ["$price", #1]}}, '\
                                        '$sort: {name: 1}'\
                                        '}'

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
            },
            {
                "$group": {
                    "_id": "$name",
                    "type": "type",
                    "total": {
                        "$sum": {
                            "$multiply": [
                                "$price",
                                "$count"
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {
                    "name": 1,
                    "count": -1
                }
            }
        ]
        assert to_aggregate(example) == '$aggregate: {'\
                                        '$match: {$and: ["size": {$eq: #0}, "count": {$lt: #1}, '\
                                        '"name": {$regex: #2}]}, '\
                                        '$group: {_id: "$name", type: #3, total: {$sum: {$multiply: '\
                                        '["$price", "$count"]}}}, '\
                                        '$sort: {name: 1, count: -1}'\
                                        '}'
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise