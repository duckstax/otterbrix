#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>


#include "components/index/index.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;

TEST_CASE("single_field_index") {
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    auto id = make_index<single_field_index_t>(index_engine, {"count"});
    insert_one(index_engine,id,gen_doc(1));
    result_set_t set(resource);
    query_t query(resource);
    query.append("count");
    find(index_engine,query,&set);

}