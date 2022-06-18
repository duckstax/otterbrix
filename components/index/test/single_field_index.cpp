#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>


#include "components/index/index.hpp"
#include "components/index/single_field_index.hpp"

using namespace components::index;

TEST_CASE("single_field_index") {
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    make_index<single_field_index_t>(index_engine, {"1"});
    auto* index = insert(index_engine, {"1"},);

}