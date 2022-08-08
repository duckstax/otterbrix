#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>

#include "components/index/index.hpp"

using namespace components::index;

class dummy final : public index_t {
public:
    explicit dummy(actor_zeta::detail::pmr::memory_resource* resource,const keys_base_t& keys)
        : index_t(resource,keys){
    }
    void insert_impl(value_t key, document_ptr value)  {}
    void find_impl(query_t,result_set_t*)  {}
};

TEST_CASE("base index created") {
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    auto one_id =  make_index<dummy>(index_engine, {"1"});
    auto two_id = make_index<dummy>(index_engine, {"1", "2"});
    auto two_1_id = make_index<dummy>(index_engine, {"2", "1"});
    REQUIRE(index_engine->size() == 3);
    auto* one = search_index(index_engine,one_id );
    auto* two = search_index(index_engine,two_id );
    auto* two_1 = search_index(index_engine,two_1_id );
}