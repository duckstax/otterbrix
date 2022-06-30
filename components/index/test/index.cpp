#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <iostream>

#include "components/index/index.hpp"

using namespace components::index;

class dummy final : public index_t {
public:
    explicit dummy(actor_zeta::detail::pmr::memory_resource* resource)
        : index_t(resource) {
    }
    auto insert_impl(key_t key, value_t) -> void {}
    auto find_impl(query_t) -> result_set_t {}
};

TEST_CASE("base index created") {
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    make_index<dummy>(index_engine, {"1"});
    make_index<dummy>(index_engine, {"1", "2"});
    make_index<dummy>(index_engine, {"2", "1"});
    REQUIRE(index_engine->size() == 3);
    auto* one = search_index(index_engine, {"1"});
    auto* two = search_index(index_engine, {"1", "2"});
    auto* two_1 = search_index(index_engine, {"2", "1"});
}