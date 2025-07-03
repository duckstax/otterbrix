#include <catch2/catch.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

TEST_CASE("sql::database") {
    auto resource = std::pmr::synchronized_pool_resource();
    components::sql::transform::transformer transformer(&resource);
    components::logical_plan::parameter_node_t statement(&resource);

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

    SECTION("drop") {
        auto drop = raw_parser("DROP DATABASE db_name;")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_database: db_name)_");
    }
}

TEST_CASE("sql::table") {
    auto resource = std::pmr::synchronized_pool_resource();
    transform::transformer transformer(&resource);
    components::logical_plan::parameter_node_t statement(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser("CREATE TABLE uuid.db_name.schema.table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create with schema") {
        auto create = raw_parser("CREATE TABLE db_name.schema.table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

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

    SECTION("drop with uuid") {
        auto drop = raw_parser("DROP TABLE uuid.db_name.schema.table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("drop with schema") {
        auto drop = raw_parser("DROP TABLE db_name.schema.table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().schema == "schema");
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

TEST_CASE("sql::index") {
    auto resource = std::pmr::synchronized_pool_resource();
    transform::transformer transformer(&resource);
    components::logical_plan::parameter_node_t statement(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser("CREATE INDEX some_idx ON uuid.db.schema.table (field);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create with schema") {
        auto create = raw_parser("CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create") {
        auto create = raw_parser("CREATE INDEX some_idx ON db.table (field);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
    }

    SECTION("drop with uuid") {
        auto drop = raw_parser("DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }
    SECTION("drop with schema") {
        auto drop = raw_parser("DROP INDEX db.schema.table.some_idx")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }
    SECTION("drop") {
        auto drop = raw_parser("DROP INDEX db.table.some_idx")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
    }
}
