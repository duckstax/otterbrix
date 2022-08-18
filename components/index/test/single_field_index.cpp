#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>

#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/ql/parser.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;

TEST_CASE("single_field_index:base") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    single_field_index_t index(resource, {"count"});
    for (int i : {0, 1, 10, 5, 6, 2, 8, 13}) {
        auto doc = gen_doc(i);
        document_view_t view(doc);
        index.insert(document::wrapper_value_t(view.get_value("count")), doc);
    }
    {
        int count = 0;
        for (auto it = index.cbegin(); it != index.cend(); ++it) {
            ++count;
        }
        REQUIRE(count == 8);
    }
    {
        auto value = ::document::impl::new_value(10);
        auto find_range = index.find(components::index::value_t(value));
        REQUIRE(find_range.first != index.cend());
        REQUIRE(document_view_t(*find_range.first).get_long("count") == 10);
        REQUIRE(document_view_t(*find_range.first).get_string("countStr") == "10");
        ++find_range.first;
        REQUIRE(find_range.first == find_range.second);
    }
}

TEST_CASE("single_field_index:engine") {
    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    auto id = make_index<single_field_index_t>(index_engine, {"count"});
    insert_one(index_engine, id, gen_doc(0));
    std::pmr::vector<document_ptr> data;
    for (int i = 10; i >= 1; --i) {
        data.push_back(gen_doc(i));
    }

    insert(index_engine, id, data);
    auto address = actor_zeta::address_t::empty_address();
    result_set_t set(resource, address);

    std::string value = R"({"count": {"$gt": 10}})";
    auto d = components::document::document_from_json(value);
    auto condition = components::ql::parse_find_condition(d);
    ///condition->type_
    /// find(index_engine, query, &set);
}