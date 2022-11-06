import pytest
import time
from ottergon import ObjectId


def test_generate():
    assert len(ObjectId().valueOf()) == 24
    assert ObjectId(2000).getTimestamp() == 2000
    assert ObjectId().getTimestamp() == int(time.time())
    assert ObjectId('12345678123456789abcdef0').valueOf() == '12345678123456789abcdef0'
