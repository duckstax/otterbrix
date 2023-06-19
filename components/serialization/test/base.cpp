#include <catch2/catch.hpp>
#include <components/serialization/serialization.hpp>


using components::serialization::serialize;
using components::serialization::stream_byte;

TEST_CASE("pod 1") {
    int d;
    stream_byte flat_stream;
    serialize(flat_stream, d, 0);
}