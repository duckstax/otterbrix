#include <catch2/catch.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::types;

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

    SECTION("typed columns") {
        auto create = linitial(raw_parser("CREATE TABLE table_name(test integer, test1 string)"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);

        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->schema().child_types()[0].type() == logical_type::INTEGER);
        REQUIRE(data->schema().child_types()[0].alias() == "test");
        REQUIRE(data->schema().child_types()[1].type() == logical_type::STRING_LITERAL);
        REQUIRE(data->schema().child_types()[1].alias() == "test1");
    }

    SECTION("typed nested") {
        auto create = linitial(raw_parser("CREATE TABLE table_name(test map<int, float>, test1 list<bit>)"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        auto map = data->schema().child_types()[0];
        auto list = data->schema().child_types()[1];

        REQUIRE(map.type() == logical_type::MAP);
        REQUIRE(map.alias() == "test");
        REQUIRE(complex_logical_type::contains(map, [](const complex_logical_type& type) -> bool {
            return type.alias() == "key" && type.type() == logical_type::INTEGER;
        }));
        REQUIRE(complex_logical_type::contains(map, [](const complex_logical_type& type) -> bool {
            return type.alias() == "value" && type.type() == logical_type::FLOAT;
        }));

        REQUIRE(list.type() == logical_type::LIST);
        REQUIRE(list.alias() == "test1");
        REQUIRE(list.child_type() == logical_type::BIT);
        REQUIRE(list.child_type().alias() == "node");
    }

    SECTION("typed struct") {
        auto create = linitial(
            raw_parser("CREATE TABLE table_name(test1 struct<blob, uint, uhugeint, timestamp_sec, decimal(5, 4)>)"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        auto sch = data->schema().child_types()[0];

        REQUIRE(sch.type() == logical_type::STRUCT);
        REQUIRE(sch.alias() == "test1");
        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "1" && type.type() == logical_type::BLOB;
        }));
        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "2" && type.type() == logical_type::UINTEGER;
        }));
        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "3" && type.type() == logical_type::UHUGEINT;
        }));
        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "4" && type.type() == logical_type::TIMESTAMP_SEC;
        }));
        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::DECIMAL) {
                return false;
            }
            auto decimal = static_cast<decimal_logical_type_extention*>(type.extention());
            return type.alias() == "5" && decimal->width() == 5 && decimal->scale() == 4;
        }));
    }

    SECTION("typed array") {
        auto create =
            linitial(raw_parser("CREATE TABLE table_name(test1 struct<decimal(51, 3)[10], int[100], boolean[8]>)"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        auto sch = data->schema().child_types()[0];

        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }

            auto array = static_cast<array_logical_type_extention*>(type.extention());
            if (array->internal_type() != logical_type::DECIMAL) {
                return false;
            }

            auto decimal = static_cast<decimal_logical_type_extention*>(array->internal_type().extention());
            return type.alias() == "1" && decimal->width() == 51, decimal->scale() == 3 && array->size() == 10;
        }));

        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }
            auto array = static_cast<array_logical_type_extention*>(type.extention());
            return type.alias() == "2" && array->internal_type() == logical_type::INTEGER && array->size() == 100;
        }));

        REQUIRE(complex_logical_type::contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }
            auto array = static_cast<array_logical_type_extention*>(type.extention());
            return type.alias() == "3" && array->internal_type() == logical_type::BOOLEAN && array->size() == 8;
        }));
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
