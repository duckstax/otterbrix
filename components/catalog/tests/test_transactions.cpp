#include <catch2/catch.hpp>

#include "catalog/catalog.hpp"
#include "utils.hpp"

#include <memory_resource>

using namespace test;
using namespace components::types;
using namespace components::catalog;

TEST_CASE("catalog::transactions::commit_abort") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    collection_full_name_t full{"db", "name"};
    create_single_column_table(full, logical_type::BIGINT, cat, &mr);

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction().add_column("new_col", complex_logical_type(logical_type::HUGEINT));
        scope.commit();
        REQUIRE_THROWS(scope.commit());
        REQUIRE_THROWS(scope.abort());
    }
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);
        REQUIRE(new_schema.find_field("new_col").type() == logical_type::HUGEINT);
    }

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction().rename_column("new_col", "new_col_1");
        // aborts...
    }
    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction().rename_column("new_col", "new_col_1");
        scope.abort();
        REQUIRE_THROWS(scope.transaction().rename_column("new_col", "new_new_col"));
        REQUIRE_THROWS(scope.commit());
    }
    // new_col still exists
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);
        REQUIRE(new_schema.find_field("new_col").type() == logical_type::HUGEINT);
    }
}

TEST_CASE("catalog::transactions::changes") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    collection_full_name_t full{"db", "name"};
    {
        auto column = complex_logical_type(logical_type::BIGINT);
        column.set_alias("col");
        schema sch(&mr, create_struct({column}, {field_description(1, false, "test")}), {1});
        cat.create_table({&mr, full}, {&mr, sch});
    }

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction()
            .add_column("new_col", complex_logical_type(logical_type::STRING_LITERAL), false, "test1")
            .rename_column("col", "new_old_col")
            .make_optional("new_old_col")
            .update_column_type("new_old_col", logical_type::HUGEINT);

        scope.commit();
    }
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);

        REQUIRE(new_schema.find_field("new_col").type() == logical_type::STRING_LITERAL);
        REQUIRE(new_schema.get_field_description("new_col").doc == "test1");

        REQUIRE(new_schema.find_field("new_old_col").type() == logical_type::HUGEINT);
        REQUIRE(new_schema.get_field_description("new_old_col").doc == "test");
    }
}

TEST_CASE("catalog::transactions::savepoints") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    collection_full_name_t full{"db", "name"};
    create_single_column_table(full, logical_type::BIGINT, cat, &mr);

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction()
            .savepoint("nothing")
            .add_column("new_col", complex_logical_type(logical_type::STRING_LITERAL), false, "test1")
            .rename_column("name", "new_old_col")
            .rollback_to_savepoint("nothing");

        scope.commit();
    }
    REQUIRE(cat.get_table_schema({&mr, full}).find_field("name").type() == logical_type::BIGINT);

    {
        auto scope = cat.begin_transaction({&mr, full});
        scope.transaction()
            .add_column("new_col", complex_logical_type(logical_type::STRING_LITERAL), false, "test1")
            .savepoint("new_column")
            .rename_column("name", "new_old_col")
            .savepoint("rename")
            .update_column_type("new_old_col", logical_type::HUGEINT)
            .rollback_to_savepoint("new_column")
            .rollback_to_savepoint("rename");

        scope.commit();
    }
    {
        auto new_schema = cat.get_table_schema({&mr, full});
        REQUIRE(new_schema.columns().size() == 2);
        REQUIRE(new_schema.find_field("new_old_col").type() == logical_type::BIGINT);
    }
}

TEST_CASE("catalog::transactions::edge_cases") {
    auto mr = std::pmr::synchronized_pool_resource();

    SECTION("catalog_destroyed") {
        auto cat = std::make_unique<catalog>(&mr);

        cat->create_namespace({"db"});
        REQUIRE(cat->namespace_exists({"db"}));

        collection_full_name_t full{"db", "name"};
        create_single_column_table(full, logical_type::BIGINT, *cat, &mr);

        {
            auto scope = cat->begin_transaction({&mr, full});
            cat.reset(nullptr);
            REQUIRE_THROWS(scope.commit());
            // must be abortable
        }
    }

    SECTION("changes during transaction") {
        catalog cat(&mr);

        cat.create_namespace({"db"});
        REQUIRE(cat.namespace_exists({"db"}));

        collection_full_name_t full{"db", "name"};
        create_single_column_table(full, logical_type::BIGINT, cat, &mr);
        {
            auto scope = cat.begin_transaction({&mr, full});
            scope.transaction().make_optional("name");
            cat.rename_table({&mr, full}, "name1");

            REQUIRE_THROWS(scope.commit());
        }

        {
            auto scope = cat.begin_transaction(table_id(&mr, table_namespace_t{"db", "name1"}));
            scope.transaction().make_optional("name");
            cat.drop_namespace({"db"});

            REQUIRE_THROWS(scope.commit());
        }
    }
}
