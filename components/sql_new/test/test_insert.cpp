#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <catch2/catch.hpp>
#include <components/sql_new/parser/parser.h>
#include <logical_plan/node_insert.hpp>
#include <ql/ql_param_statement.hpp>

using namespace components::sql_new;

TEST_CASE("sql_new::insert_into") {
    auto resource = std::pmr::synchronized_pool_resource();
    transform::transformer transformer(&resource);

    SECTION("insert into with TestDatabase") {
        components::ql::ql_param_statement_t agg(&resource);
        auto select = raw_parser("INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES (1, 'Name', 1);")
                          ->lst.front()
                          .data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "testdatabase");
        REQUIRE(node->collection_name() == "testcollection");
        REQUIRE(static_cast<components::logical_plan::node_insert_t&>(*node).documents().size() == 1);
        auto doc = static_cast<components::logical_plan::node_insert_t&>(*node).documents().front();
        REQUIRE(doc->get_long("id") == 1);
        REQUIRE(doc->get_string("name") == "Name");
        REQUIRE(doc->get_long("count") == 1);
    }

    SECTION("insert into without TestDatabase") {
        components::ql::ql_param_statement_t agg(&resource);
        auto select =
            raw_parser("INSERT INTO TestCollection (id, name, count) VALUES (1, 'Name', 1);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        REQUIRE(static_cast<components::logical_plan::node_insert_t&>(*node).documents().size() == 1);
        auto doc = static_cast<components::logical_plan::node_insert_t&>(*node).documents().front();
        REQUIRE(doc->get_long("id") == 1);
        REQUIRE(doc->get_string("name") == "Name");
        REQUIRE(doc->get_long("count") == 1);
    }

    SECTION("insert into with quoted") {
        components::ql::ql_param_statement_t agg(&resource);
        auto select =
            raw_parser(R"(INSERT INTO TestCollection (id, "name", "count") VALUES (1, 'Name', 1);)")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        REQUIRE(static_cast<components::logical_plan::node_insert_t&>(*node).documents().size() == 1);
        auto doc = static_cast<components::logical_plan::node_insert_t&>(*node).documents().front();
        REQUIRE(doc->get_long("id") == 1);
        REQUIRE(doc->get_string("name") == "Name");
        REQUIRE(doc->get_long("count") == 1);
    }

    SECTION("insert into multi-documents") {
        components::ql::ql_param_statement_t agg(&resource);
        auto select = raw_parser("INSERT INTO TestCollection (id, name, count) VALUES "
                                 "(1, 'Name1', 1), "
                                 "(2, 'Name2', 2), "
                                 "(3, 'Name3', 3), "
                                 "(4, 'Name4', 4), "
                                 "(5, 'Name5', 5);")
                          ->lst.front()
                          .data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(select), &agg);
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(node->database_name() == "");
        REQUIRE(node->collection_name() == "testcollection");
        REQUIRE(static_cast<components::logical_plan::node_insert_t&>(*node).documents().size() == 5);
        auto doc1 = static_cast<components::logical_plan::node_insert_t&>(*node).documents().front();
        REQUIRE(doc1->get_long("id") == 1);
        REQUIRE(doc1->get_string("name") == "Name1");
        REQUIRE(doc1->get_long("count") == 1);
        auto doc5 = static_cast<components::logical_plan::node_insert_t&>(*node).documents().back();
        REQUIRE(doc5->get_long("id") == 5);
        REQUIRE(doc5->get_string("name") == "Name5");
        REQUIRE(doc5->get_long("count") == 5);
    }
}