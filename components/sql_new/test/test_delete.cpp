#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <catch2/catch.hpp>
#include <components/sql_new/parser/parser.h>
#include <ql/ql_param_statement.hpp>

using namespace components::sql_new;

#define TEST_SIMPLE_DELETE(QUERY, RESULT, PARAMS)                                                                      \
    SECTION(QUERY) {                                                                                                   \
        auto resource = std::pmr::synchronized_pool_resource();                                                        \
        transform::transformer transformer(&resource);                                                                 \
        components::ql::ql_param_statement_t agg(&resource);                                                           \
        auto select = raw_parser(QUERY)->lst.front().data;                                                      \
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);                              \
        REQUIRE(node->type() == components::logical_plan::node_type::delete_t);                                                    \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg.parameters().parameters.size() == PARAMS.size());                                                  \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(agg.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                 \
        }                                                                                                              \
    }

using v = components::document::value_t;
using vec = std::vector<v>;

TEST_CASE("sql_new::delete_from_where") {
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
