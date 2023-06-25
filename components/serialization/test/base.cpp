#include <catch2/catch.hpp>
#include <components/serialization/serialization.hpp>
#include <components/serialization/traits.hpp>


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


void tag_impl(components::serialization::array_tag){
    std::cerr << "array_tag\n";
}


void tag_impl(components::serialization::object_tag){
    std::cerr << "map_tag\n";
}

template<class C>
void tag(C&&c){
    tag_impl(components::serialization::serialization_trait<C>::tag());
}



TEST_CASE("iterator") {
    tag(bool{1});
}