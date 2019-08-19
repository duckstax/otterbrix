from pyrocketjoe import MapReduce


def map_impl(data: str) -> str:
    return data


context = MapReduce.RocketJoeContext("Test")
result = context \
    .textFile("big_data.txt") \
    .flatMap(map_impl) \
    .map(map_impl) \
    .reduceByKey(map_impl) \
    .collect()

print(result)
