#include <catch2/catch.hpp>

#include "utils.hpp"

#include <memory_resource>
#include <string>

using namespace test;
using namespace components::types;
using namespace components::catalog;

TEST_CASE("catalog::schema_test") {
    auto mr = std::pmr::synchronized_pool_resource();
    catalog cat(&mr);

    cat.create_namespace({"db"});
    REQUIRE(cat.namespace_exists({"db"}));

    SECTION("single_table") {
        collection_full_name_t full{"db", "name"};
        create_single_column_table(full, logical_type::BIGINT, cat, &mr);
        REQUIRE(cat.table_exists({&mr, full}));

        auto tbl = cat.get_table_schema({&mr, full});
        REQUIRE(tbl.find_field("name").type() == logical_type::BIGINT);

        auto desc = tbl.get_field_description("name");
        REQUIRE(desc.required == true);
        REQUIRE(desc.doc == "test");
        REQUIRE(desc.field_id == 1);

        REQUIRE(cat.list_tables({"db"}).front().table_name() == "name");
    }

    SECTION("more_fields_and_tables") {
        collection_full_name_t full{"db", "fields"};

        std::vector<complex_logical_type> fields;
        fields.emplace_back(logical_type::BOOLEAN);
        fields.back().set_alias("flag");
        fields.emplace_back(logical_type::INTEGER);
        fields.back().set_alias("number");
        fields.emplace_back(logical_type::STRING_LITERAL);
        fields.back().set_alias("name");
        fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT));
        fields.back().set_alias("array");

        std::vector<field_description> desc{{1}};
        auto sch = schema(&mr, create_struct(fields, n_field_descriptions<4>()));

        cat.create_table({&mr, full}, {&mr, sch});
        REQUIRE(cat.table_exists({&mr, full}));

        auto tbl = cat.get_table_schema({&mr, full});
        REQUIRE(tbl.find_field("flag").type() == logical_type::BOOLEAN);
        REQUIRE(tbl.find_field("number").type() == logical_type::INTEGER);
        REQUIRE(tbl.find_field("name").type() == logical_type::STRING_LITERAL);
        REQUIRE(tbl.find_field("array").type() == logical_type::LIST);

        REQUIRE(tbl.get_field_description("flag").field_id == 1);
        REQUIRE(tbl.get_field_description("number").field_id == 2);
        REQUIRE(tbl.get_field_description("name").field_id == 3);
        REQUIRE(tbl.get_field_description("array").field_id == 4);

        for (size_t i = 0; i < 10; ++i) {
            collection_full_name_t full_n{"db", "fields" + std::to_string(i)};
            cat.create_table({&mr, full_n}, {&mr, sch});
        }

        auto tbls = cat.list_tables({"db"}); // sorted with std::map
        REQUIRE(tbls.front().table_name() == "fields");

        for (size_t i = 1; i < 11; ++i) {
            REQUIRE(tbls[i].table_name() == std::pmr::string("fields" + std::to_string(i - 1)));
        }
    }
}

TEST_CASE("catalog::trie_test") {
    SECTION("correctness") {
        auto mr = std::pmr::synchronized_pool_resource();
        catalog cat(&mr);

        cat.create_namespace({"1"});
        cat.create_namespace({"2"});
        cat.create_namespace({"3"});
        cat.create_namespace({"2", "3"});
        cat.create_namespace({"2", "3", "4"});
        cat.create_namespace({"2", "4"});
        cat.create_namespace({"1", "2"});

        auto children = cat.list_namespaces({"2"});
        REQUIRE(children.size() == 2);
        REQUIRE(children[0] == table_namespace_t{"2", "3"});
        REQUIRE(children[1] == table_namespace_t{"2", "4"});

        REQUIRE(cat.list_namespaces() == std::pmr::vector<table_namespace_t>{{"1"}, {"2"}, {"3"}});

        cat.drop_namespace({"2", "3", "4"});
        cat.drop_namespace({"1", "2"});

        size_t cnt = 0;
        std::pmr::vector<table_namespace_t> dfs(cat.list_namespaces());
        while (!dfs.empty()) {
            auto cur = dfs.back();
            dfs.pop_back();

            ++cnt;
            REQUIRE(cat.namespace_exists(cur));
            auto next = cat.list_namespaces(cur);
            dfs.insert(dfs.end(), next.begin(), next.end());
        }

        REQUIRE(cnt == 5); // 7 total - 2 dropped
    }

    SECTION("MVCC") {
        using namespace std::string_literals;

        trie_map<std::string, int> trie;
        auto v1 = "v1"s;
        {
            trie.insert(v1, 10);
            auto it = trie.find(v1);

            trie.insert(v1, 20);
            trie.insert(v1, 30);
            trie.insert(v1, 40);
            REQUIRE(trie.find(v1)->value == 40);

            trie.erase(v1);
            REQUIRE(trie.find(v1)->value == 30);
            REQUIRE(it->value == 10);

            trie.erase(v1);
            REQUIRE(trie.find(v1)->value == 20);
            REQUIRE(it->value == 10);
        }

        // 10 lost all its references, 20 is being deleted -> trie is empty
        trie.erase(v1);
        REQUIRE(trie.find(v1) == trie.end());
        REQUIRE(trie.empty());
    }
}
