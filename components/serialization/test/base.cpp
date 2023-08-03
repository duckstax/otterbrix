#include <catch2/catch.hpp>
#include <components/serialization/serialization.hpp>
#include <components/serialization/traits.hpp>
#include <components/serialization/stream/json.hpp>

#include <map>
#include <vector>
#include <string>
#include <string_view>

using components::serialization::stream::output_stream_json;
using components::serialization::serialize;
using components::serialization::serialize_to_array;

TEST_CASE("example new 1") {
    output_stream_json flat_stream;
    std::vector<int64_t> vector{1,2,3};
    std::map<int64_t ,int64_t> map = {{1,2},{3,4}};
    std::string str("42");
    std::string_view str1("42");
    std::uint64_t number = 42;
    serialize(flat_stream,serialize_to_array, 5);
    serialize(flat_stream, number);
    serialize(flat_stream, str);
    serialize(flat_stream, str1);
    serialize(flat_stream, vector);
    serialize(flat_stream, map);
    std::cerr << flat_stream.data() << std::endl;
}