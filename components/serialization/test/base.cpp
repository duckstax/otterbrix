#include <catch2/catch.hpp>
#include <components/serialization/serialization.hpp>


using components::serialization::serialize;
using components::serialization::serialize_array;
using components::serialization::stream_byte;
using components::serialization::stream_json;

/*
TEST_CASE("pod 1") {
    int d;
    stream_byte flat_stream;
    serialize_array(flat_stream, 1, 0);
    serialize(flat_stream, 42, 0);
}
*/
TEST_CASE("base string") {
    stream_json flat_stream;
    serialize_array(flat_stream, 1);
    serialize(flat_stream, std::string("42"));
    std::cout<< flat_stream.data() << std::endl;
}

TEST_CASE("base vector") {
    stream_json flat_stream;
    std::vector<int64_t> data{1,2,3};
    serialize_array(flat_stream, 1);
    serialize(flat_stream, data);
    std::cout<< flat_stream.data() << std::endl;
}