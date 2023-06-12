#include <catch2/catch.hpp>

#include <map>
#include <vector>

#include <components/serialization/serialization.hpp>

using components::serialization::serialize;
using components::serialization::stream_json;

/*
    template<typename InputType>
    bool from_msgpack(InputType&& i ) ;

    template<typename IteratorType>
    bool from_msgpack(IteratorType first, IteratorType last) ;

    std::vector<std::uint8_t> to_msgpack(const document_t& j);
    void to_msgpack(const document_t& j, output_adapter<std::uint8_t> o);
    void to_msgpack(const document_t& j, output_adapter<char> o);
*/
template<class C>
bool to_json(C& c) {

}

class no_dto_t {
public:
    no_dto_t() = default;

    void serialize(stream_json& ar, const unsigned int version) {
        ::serialize(ar, example_0_, version);
        ::serialize(ar, example_1_, version);
        ::serialize(ar, example_2_, version);
        ::serialize(ar, example_3_, version);
    }

private:
    std::uint64_t example_0_;
    std::string example_1_;
    std::vector<uint64_t> example_2_;
    std::map<std::string_view, uint64_t> example_3_;
};

struct dto_t {
    std::uint64_t example_0_;
    std::string example_1_;
    std::vector<uint64_t> example_2_;
    std::map<std::string_view, uint64_t> example_3_;
};

void serialize(stream_json& ar, dto_t& dto, const unsigned int version) {
    serialize(ar, dto.example_0_, version);
    serialize(ar, dto.example_1_, version);
    serialize(ar, dto.example_2_, version);
    serialize(ar, dto.example_3_, version);
}

TEST_CASE("big_example 1") {
    no_dto_t no_dto;
    stream_json flat_stream;
    no_dto.serialize(flat_stream, 0);
}

TEST_CASE("big_example 2") {
    dto_t dto;
    stream_json flat_stream;
    serialize(flat_stream, dto, 0);
}