#include <catch2/catch.hpp>
#include <components/serialization/serialization.hpp>
#include <components/serialization/stream/json.hpp>

using components::serialization::serialize;
using components::serialization::serialize_array;
using components::serialization::stream::stream_json;


TEST_CASE("int") {
    stream_json flat_stream;
    serialize_array(flat_stream, 1);
    ///serialize(flat_stream, 42);
    std::cout<< flat_stream.data() << std::endl;
}

TEST_CASE("base string") {
    stream_json flat_stream;
    serialize_array(flat_stream, 1);
    serialize(flat_stream, std::string("42"));
    std::cout<< flat_stream.data() << std::endl;
}

TEST_CASE("base vector") {
    stream_json flat_stream;
    std::vector<int64_t> data{1,2,3};
    serialize_array(flat_stream, 2);
    serialize(flat_stream, std::string("42"));
    serialize(flat_stream, data);
    std::cout<< flat_stream.data() << std::endl;
}


TEST_CASE("base map") {
    stream_json flat_stream;
    std::vector<int64_t> vector{1,2,3};
    std::map<int64_t ,int64_t> map = {{1,2},{3,4}};
    serialize_array(flat_stream, 5);
    serialize(flat_stream, std::int64_t(42));
    serialize(flat_stream, std::string_view("42"));
    serialize(flat_stream, std::string("42"));
    serialize(flat_stream, vector);
    serialize(flat_stream, map);
    std::cout<< flat_stream.data() << std::endl;
}