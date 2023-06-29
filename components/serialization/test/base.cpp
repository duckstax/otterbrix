#include <catch2/catch.hpp>
///#include <components/serialization/serialization.hpp>
///#include <components/serialization/traits.hpp>
///#include <components/serialization/stream/json.hpp>
///#include <type_traits>
///#include <iostream>
/*
template<typename Container>
using IteratorCategoryOf = typename std::iterator_traits<typename Container::iterator>::iterator_category;

template<typename Container>
void algorithm(Container &c, std::forward_iterator_tag) {
    std::cerr << "forward iterator\n";
}

template<typename Container>
void algorithm(Container &c, std::random_access_iterator_tag) {
    std::cerr << "random access iterator\n";
}

template<typename Container>
void algorithm(Container &c) {
    algorithm(
        c,
        IteratorCategoryOf<Container>());
}
 */
/*
template<typename Container>
void tag_impl(Container&&c,components::serialization::array_tag){
    std::cerr << "array_tag\n";
}

template<typename Container>
void tag_impl(Container&&c,components::serialization::object_tag){
    std::cerr << "map_tag\n";
}

template<class C>
void tag(C&&c){
    using t = std::decay_t<C>;
    tag_impl(c,typename components::serialization::serialization_trait<t>::category());
}

*/

/*
using components::serialization::serialize;
using components::serialization::serialize_array;
using components::serialization::stream::stream_json;

TEST_CASE("iterator") {

    stream_json flat_stream;
    std::vector<int64_t> vector{1,2,3};
    std::map<int64_t ,int64_t> map = {{1,2},{3,4}};
    std::string str("42");
    std::string_view str1("42");
    serialize_array(flat_stream, 5);
    serialize(flat_stream, std::int64_t(42));
    serialize(flat_stream, str);
    serialize(flat_stream, str1);
    serialize(flat_stream, vector);
    serialize(flat_stream, map);
    std::cout<< flat_stream.data() << std::endl;
}*/
#include <map>
#include <vector>
#include <string>
#include <string_view>
class stream_json{};

template<class T>
class stream;

template<class Storage>
void serialize_array(stream<Storage>& ar, std::size_t size);

uint64_t as_uint64();

TEST_CASE("example new 1") {
    stream_json flat_stream;
    std::vector<int64_t> vector{1,2,3};
    std::map<int64_t ,int64_t> map = {{1,2},{3,4}};
    std::string str("42");
    std::string_view str1("42");
    std::uint64_t number = 42;
    serialize_array(flat_stream, 5);
    serialize(flat_stream, as_uint64(number));
    serialize(flat_stream, as_string(str));
    serialize(flat_stream, as_string(str1));
    serialize(flat_stream, as_vector(vector));
    serialize(flat_stream, as_map(map));
}

TEST_CASE("example new 2") {
    stream_json flat_stream;
    std::vector<int64_t> vector{1,2,3};
    std::map<int64_t ,int64_t> map = {{1,2},{3,4}};
    std::string str("42");
    std::string_view str1("42");
    std::uint64_t number = 42;
    serialize_array(flat_stream, 5);
    serialize(flat_stream, as_uint64(number));
    serialize(flat_stream, as_string(str));
    serialize(flat_stream, as_string(str1));
    serialize(flat_stream, as_vector(vector));
    serialize(flat_stream, as_map(map));
}