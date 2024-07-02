#include <catch2/catch.hpp>
#include <components/document/document.hpp>
#include <components/sql/parser.hpp>
#include <iostream>

using namespace components;

#define TEST_SIMPLE_UPDATE(QUERY, RESULT, PARAMS, FIELDS)                                                              \
    SECTION(QUERY) {                                                                                                   \
        auto res = sql::parse(resource, QUERY);                                                                        \
        auto ql = res.ql;                                                                                              \
        REQUIRE(std::holds_alternative<ql::update_many_t>(ql));                                                        \
        auto& upd = std::get<ql::update_many_t>(ql);                                                                   \
        REQUIRE(upd.database_ == "schema");                                                                            \
        REQUIRE(upd.collection_ == "table");                                                                           \
        std::stringstream s;                                                                                           \
        s << upd.match_;                                                                                               \
        REQUIRE(s.str() == RESULT);                                                                                    \
        REQUIRE(upd.parameters().parameters.size() == PARAMS.size());                                                  \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(upd.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                 \
        }                                                                                                              \
        auto doc = upd.update_;                                                                                        \
        auto view = doc->get_dict("$set");                                                                             \
        REQUIRE(view->count() == FIELDS.size());                                                                       \
        for (auto f : FIELDS) {                                                                                        \
            REQUIRE(view->is_equals(f.first, f.second));                                                               \
        }                                                                                                              \
    }

#define TEST_NO_VALID_UPDATE(QUERY)                                                                                    \
    SECTION(QUERY) {                                                                                                   \
        auto query = QUERY;                                                                                            \
        auto res = sql::parse(resource, query);                                                                        \
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));                                               \
    }

#define TEST_ERROR_UPDATE(QUERY, ERROR, TEXT, POS)                                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto query = QUERY;                                                                                            \
        auto res = sql::parse(resource, query);                                                                        \
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));                                               \
        REQUIRE(res.error.error() == ERROR);                                                                           \
        REQUIRE(res.error.mistake() == TEXT);                                                                          \
        REQUIRE(res.error.mistake().data() == query + POS);                                                            \
    }

using v = document::value_t;
using vec = std::vector<v>;
using fields = std::vector<std::pair<std::string, v>>;

TEST_CASE("parser::update") {
    auto* resource = std::pmr::get_default_resource();
    auto tape = std::make_unique<document::impl::mutable_document>(resource);
    auto new_value = [&](auto value) { return v{resource, tape.get(), value}; };

    TEST_SIMPLE_UPDATE("update schema.table set count = 10;",
                       R"_($match: {$all_true})_",
                       vec({}),
                       fields({{"count", new_value(10ul)}}));

    TEST_SIMPLE_UPDATE("update schema.table set name = 'new name';",
                       R"_($match: {$all_true})_",
                       vec({}),
                       fields({{"name", new_value(std::pmr::string("new name", resource))}}));

    TEST_SIMPLE_UPDATE("update schema.table set is_doc = true;",
                       R"_($match: {$all_true})_",
                       vec({}),
                       fields({{"is_doc", new_value(true)}}));

    TEST_SIMPLE_UPDATE("update schema.table set is_doc = false;",
                       R"_($match: {$all_true})_",
                       vec({}),
                       fields({{"is_doc", new_value(false)}}));

    TEST_SIMPLE_UPDATE("update schema.table set count = 10, name = 'new name', is_doc = true;",
                       R"_($match: {$all_true})_",
                       vec({}),
                       fields({{"count", new_value(10ul)},
                               {"name", new_value(std::pmr::string("new name"))},
                               {"is_doc", new_value(true)}}));
}

TEST_CASE("parser::update_where") {
    auto* resource = std::pmr::get_default_resource();
    auto tape = std::make_unique<document::impl::mutable_document>(resource);
    auto new_value = [&](auto value) { return v{resource, tape.get(), value}; };

    TEST_SIMPLE_UPDATE("update schema.table set count = 10 where id = 1;",
                       R"_($match: {"id": {$eq: #0}})_",
                       vec({new_value(1l)}),
                       fields({{"count", new_value(10ul)}}));

    TEST_SIMPLE_UPDATE("update schema.table set name = 'new name' where name = 'old_name';",
                       R"_($match: {"name": {$eq: #0}})_",
                       vec({new_value(std::string_view("old_name"))}),
                       fields({{"name", new_value(std::string_view("new name"))}}));

    TEST_SIMPLE_UPDATE("update schema.table set is_doc = true where is_doc = false;",
                       R"_($match: {"is_doc": {$eq: #0}})_",
                       vec({new_value(false)}),
                       fields({{"is_doc", new_value(true)}}));

    TEST_SIMPLE_UPDATE("update schema.table set is_doc = false where id > 10;",
                       R"_($match: {"id": {$gt: #0}})_",
                       vec({new_value(10l)}),
                       fields({{"is_doc", new_value(false)}}));

    TEST_SIMPLE_UPDATE("update schema.table set count = 10, name = 'new name', is_doc = true "
                       "where id > 10 and name = 'old_name' and is_doc = false;",
                       R"_($match: {$and: ["id": {$gt: #0}, "name": {$eq: #1}, "is_doc": {$eq: #2}]})_",
                       vec({new_value(10l), new_value(std::string_view("old_name")), new_value(false)}),
                       fields({{"count", new_value(10ul)},
                               {"name", new_value(std::string_view("new name"))},
                               {"is_doc", new_value(true)}}));
}

TEST_CASE("parser::update no valid queries") {
    auto* resource = std::pmr::get_default_resource();

    TEST_NO_VALID_UPDATE("update schema.table where number == 10;");
    TEST_NO_VALID_UPDATE("update;");
}

TEST_CASE("parser::update errors") {
    auto* resource = std::pmr::get_default_resource();

    TEST_ERROR_UPDATE("update schema.table set name = 'new name',;", sql::parse_error::syntax_error, ";", 42);

    TEST_ERROR_UPDATE("update schema.table set name = 'new name' count = 10;",
                      sql::parse_error::syntax_error,
                      "count",
                      42);

    TEST_ERROR_UPDATE("update schema.table set name = 'new name' where number == 10 group by name;",
                      sql::parse_error::syntax_error,
                      "group",
                      61);

    TEST_ERROR_UPDATE("update schema. set name = 'new name' where number == 10;",
                      sql::parse_error::syntax_error,
                      " ",
                      14);

    TEST_ERROR_UPDATE("update .table set name = 'new name' where number == 10;",
                      sql::parse_error::syntax_error,
                      ".",
                      7);

    TEST_ERROR_UPDATE("update schema.table set name = 'new name' where number == 10 name = 'doc 10';",
                      sql::parse_error::syntax_error,
                      "name",
                      61);

    TEST_ERROR_UPDATE("update schema.table set name = 'new name' where number == 10 and and = 'doc 10';",
                      sql::parse_error::syntax_error,
                      "and",
                      65);
}
