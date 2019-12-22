from typing import List, Tuple
from pyrocketjoe import MapReduce


def map(data: str) -> Tuple[str, int]:
    return data, 1


def split_string(line: str) -> List[str]:
    return line.split("\n")


def reduce_by_key(a: int, b: int) -> int:
    return a + b


context = MapReduce.RocketJoeContext("Test")
counts = context \
    .textFile("big_data.txt") \
    .flatMap(split_string) \
    .map(map) \
    .reduceByKey(reduce_by_key) \

output = counts.collect()

print(output)
