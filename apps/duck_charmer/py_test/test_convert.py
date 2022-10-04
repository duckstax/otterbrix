import pytest
import time
from duck_charmer import to_statement


def test_convector():
    example = [{}]
    to_statement(example)
