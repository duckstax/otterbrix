import pytest
import time
from otterbrix import ObjectId


def test_generate():
    assert len(ObjectId().valueOf()) == 24
    assert ObjectId(2000).getTimestamp() == 2000
    assert abs(ObjectId().getTimestamp() - int(time.time())) < 3
    assert ObjectId('12345678123456789abcdef0').valueOf() == '12345678123456789abcdef0'