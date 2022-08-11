#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>

#include "components/index/index.hpp"
#include "components/index/single_field_index.hpp"
#include "components/ql/parser.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;

TEST_CASE("single_field_index") {
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    auto id = make_index<single_field_index_t>(index_engine, {"count"});
    insert_one(index_engine, id, gen_doc(0));
    std::pmr::vector<document_ptr> data;
    for (int i = 10; i >= 1; --i) {
        data.push_back(gen_doc(i));
    }

    insert(index_engine, id, data);
    result_set_t set(resource);

    std::string value = R"({"count": {"$gt": 10}})";
    auto d = components::document::document_from_json(value);
    auto condition = components::ql::parse_find_condition(d);
    condition->type_
    find(index_engine, query, &set);
}