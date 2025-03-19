#include <catch2/catch.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;

#define TEST_SIMPLE_UPDATE(QUERY, RESULT, PARAMS, FIELDS)                                                              \
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
        REQUIRE(node->database_name() == "testdatabase");                                                              \
        REQUIRE(node->collection_name() == "testcollection");                                                          \
        auto doc = static_cast<components::logical_plan::node_update_t&>(*node).update();                              \
        auto dict = doc->get_dict("$set");                                                                             \
        REQUIRE(dict->count() == FIELDS.size());                                                                       \
        for (auto f : FIELDS) {                                                                                        \
            REQUIRE(dict->is_equals(f.first, f.second));                                                               \
        }                                                                                                              \
    }

using v = components::document::value_t;
using vec = std::vector<v>;
using fields = std::vector<std::pair<std::string, v>>;

TEST_CASE("sql::update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10;",
                       R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                       vec({}),
                       fields({{"count", new_value(10ul)}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET name = 'new name';",
                       R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                       vec({}),
                       fields({{"name", new_value(std::pmr::string("new name", &resource))}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = true;",
                       R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                       vec({}),
                       fields({{"is_doc", new_value(true)}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = false;",
                       R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                       vec({}),
                       fields({{"is_doc", new_value(false)}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10, name = 'new name', is_doc = true;",
                       R"_($update: {$upsert: 0, $match: {$all_true}, $limit: -1})_",
                       vec({}),
                       fields({{"count", new_value(10ul)},
                               {"name", new_value(std::pmr::string("new name"))},
                               {"is_doc", new_value(true)}}));
}

TEST_CASE("sql::update_where") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<components::document::impl::base_document>(&resource);
    auto new_value = [&](auto value) { return v{tape.get(), value}; };

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10 WHERE id = 1;",
                       R"_($update: {$upsert: 0, $match: {"id": {$eq: #0}}, $limit: -1})_",
                       vec({new_value(1l)}),
                       fields({{"count", new_value(10ul)}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET name = 'new name' WHERE name = 'old_name';",
                       R"_($update: {$upsert: 0, $match: {"name": {$eq: #0}}, $limit: -1})_",
                       vec({new_value(std::string_view("old_name"))}),
                       fields({{"name", new_value(std::string_view("new name"))}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = true WHERE is_doc = false;",
                       R"_($update: {$upsert: 0, $match: {"is_doc": {$eq: #0}}, $limit: -1})_",
                       vec({new_value(false)}),
                       fields({{"is_doc", new_value(true)}}));

    TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = false WHERE id > 10;",
                       R"_($update: {$upsert: 0, $match: {"id": {$gt: #0}}, $limit: -1})_",
                       vec({new_value(10l)}),
                       fields({{"is_doc", new_value(false)}}));

    TEST_SIMPLE_UPDATE(
        "UPDATE TestDatabase.TestCollection SET count = 10, name = 'new name', is_doc = true "
        "WHERE id > 10 AND name = 'old_name' AND is_doc = false;",
        R"_($update: {$upsert: 0, $match: {$and: ["id": {$gt: #0}, "name": {$eq: #1}, "is_doc": {$eq: #2}]}, $limit: -1})_",
        vec({new_value(10l), new_value(std::string_view("old_name")), new_value(false)}),
        fields({{"count", new_value(10ul)},
                {"name", new_value(std::string_view("new name"))},
                {"is_doc", new_value(true)}}));
}