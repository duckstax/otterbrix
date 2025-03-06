#include "sql_new/transformer/transformer.hpp"
#include "sql_new/transformer/utils.hpp"
#include <catch2/catch.hpp>
#include <components/sql_new/parser/parser.h>
#include <ql/ql_param_statement.hpp>

using namespace components::sql_new;

TEST_CASE("sql_new::create_database") {
    auto resource = std::pmr::synchronized_pool_resource();
    transform::transformer transformer(&resource);
    components::ql::ql_param_statement_t statement(&resource);

    SECTION("create") {
        auto create = raw_parser("CREATE DATABASE db_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create;") {
        auto create = raw_parser("CREATE DATABASE db_name;")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; ") {
        auto create = raw_parser("CREATE DATABASE db_name;          ")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; --") {
        auto create = raw_parser("CREATE DATABASE db_name; -- comment")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; /*") {
        auto create = raw_parser("CREATE DATABASE db_name; /* multiline\n"
                                 "comments */")
                          ->lst.front()
                          .data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; /* mid */") {
        auto create = raw_parser("CREATE /* comment */ DATABASE db_name;")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }
}

TEST_CASE("sql_new::drop_database") {
    auto resource = std::pmr::synchronized_pool_resource();
    components::ql::ql_param_statement_t statement(&resource);

    auto drop = raw_parser("DROP DATABASE db_name;")->lst.front().data;
    transform::transformer transformer(&resource);

    auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
    REQUIRE(node->to_string() == R"_($drop_database: db_name)_");
}

TEST_CASE("sql_new::table") {
    auto resource = std::pmr::synchronized_pool_resource();
    transform::transformer transformer(&resource);
    components::ql::ql_param_statement_t statement(&resource);

    SECTION("create") {
        auto create = raw_parser("CREATE TABLE db_name.table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
    }

    SECTION("create without database") {
        auto create = raw_parser("CREATE TABLE table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: .table_name)_");
    }

    SECTION("drop") {
        auto drop = raw_parser("DROP TABLE db_name.table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
    }

    SECTION("drop without database") {
        auto drop = raw_parser("DROP TABLE table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: .table_name)_");
    }
}
