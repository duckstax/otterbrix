#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

#define TEST_SIMPLE_DELETE(QUERY, RESULT, PARAMS)                                                                      \
    SECTION(QUERY) {                                                                                                   \
        auto resource = std::pmr::synchronized_pool_resource();                                                        \
        transform::transformer transformer(&resource);                                                                 \
        components::logical_plan::parameter_node_t agg(&resource);                                                     \
        auto select = raw_parser(QUERY)->lst.front().data;                                                             \
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);                              \
        REQUIRE(node->type() == components::logical_plan::node_type::delete_t);                                        \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg.parameters().parameters.size() == PARAMS.size());                                                  \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(agg.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                 \
        }                                                                                                              \
    }

using v = components::document::value_t;
using vec = std::vector<v>;

TEST_CASE("sql::delete_from_where") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_DELETE("DELETE FROM TestDatabase.TestCollection WHERE number == 10;",
                       R"_($delete: {$match: {"number": {$eq: #0}}, $limit: -1})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_DELETE(
        "DELETE FROM TestDatabase.TestCollection WHERE NOT (number = 10) AND NOT(name = 'doc 10' OR count = 2);",
        R"_($delete: {$match: {$and: [$not: ["number": {$eq: #0}], $not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}, $limit: -1})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));
}
