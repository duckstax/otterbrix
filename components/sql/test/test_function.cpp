#include <catch2/catch.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

using v = components::document::value_t;
using vec = std::vector<v>;
using fields = std::vector<std::pair<std::string, v>>;

#define TEST_SIMPLE_FUNCTION(QUERY, RESULT, PARAMS)                                                                    \
    SECTION(QUERY) {                                                                                                   \
        auto resource = std::pmr::synchronized_pool_resource();                                                        \
        transform::transformer transformer(&resource);                                                                 \
        components::logical_plan::parameter_node_t agg(&resource);                                                     \
        auto select = raw_parser(QUERY)->lst.front().data;                                                             \
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);                              \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg.parameters().parameters.size() == PARAMS.size());                                                  \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(agg.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                 \
        }                                                                                                              \
    }
TEST_CASE("sql::function") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM func_name(some_argument);)_",
                         R"_($aggregate: {$function: {name: {"func_name"}, args: {#0}}})_",
                         vec({new_value("some_argument")}));

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM postgresSql('public.customers', "sql query") AS customer;)_",
                         R"_($aggregate: {$function: {name: {"postgressql"}, args: {#0, #1}}})_",
                         vec({new_value("public.customers"), new_value("sql query")}));

    TEST_SIMPLE_FUNCTION(R"_(SELECT * FROM no_arg() AS customer;)_",
                         R"_($aggregate: {$function: {name: {"no_arg"}, args: {}}})_",
                         vec({}));

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT alias.id, alias.count, alias.value FROM some_database('db_name.col_name', "some query") AS alias;)_",
        R"_($aggregate: {$function: {name: {"some_database"}, args: {#0, #1}}, $group: {id, count, value}})_",
        vec({new_value("db_name.col_name"), new_value("some query")}));

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT first_set.id, second_set.id FROM some_database('db_name.col_name', "some query") AS first_set )_"
        R"_(JOIN other_database('db_name.col_name', "other query") AS second_set )_"
        R"_(ON first_set.id = second_set.id;)_",
        R"_($aggregate: {$join: {$type: inner, $function: {name: {"some_database"}, args: {#0, #1}}, )_"
        R"_($function: {name: {"other_database"}, args: {#2, #3}}, "id": {$eq: "id"}}, $group: {id, id}})_",
        vec({new_value("db_name.col_name"),
             new_value("some query"),
             new_value("db_name.col_name"),
             new_value("other query")}));

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT * FROM sql_func('db.col', "select something") AS col1 JOIN col2 ON col1.id = col2.id_col1;)_",
        R"_($aggregate: {$join: {$type: inner, $function: {name: {"sql_func"}, args: {#0, #1}}, $aggregate: {}, "id": {$eq: "id_col1"}}})_",
        vec({new_value("db.col"), new_value("select something")}));

    TEST_SIMPLE_FUNCTION(
        R"_(SELECT * FROM col1 JOIN sql_func('db.col', "select something else") AS col2 ON col1.id = col2.id_col1;)_",
        R"_($aggregate: {$join: {$type: inner, $aggregate: {}, $function: {name: {"sql_func"}, args: {#0, #1}}, "id": {$eq: "id_col1"}}})_",
        vec({new_value("db.col"), new_value("select something else")}));
}