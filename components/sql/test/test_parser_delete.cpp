#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

#define TEST_SIMPLE_DELETE(QUERY, RESULT, PARAMS)                                                                      \
    SECTION(QUERY) {                                                                                                   \
        auto res = sql::parse(resource, QUERY);                                                                        \
        auto ql = res.ql;                                                                                              \
        REQUIRE(std::holds_alternative<ql::delete_many_t>(ql));                                                        \
        auto& del = std::get<ql::delete_many_t>(ql);                                                                   \
        REQUIRE(del.database_ == "schema");                                                                            \
        REQUIRE(del.collection_ == "table");                                                                           \
        std::stringstream s;                                                                                           \
        s << del.match_;                                                                                               \
        REQUIRE(s.str() == RESULT);                                                                                    \
        REQUIRE(del.parameters().parameters.size() == PARAMS.size());                                                  \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(del.parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                                 \
        }                                                                                                              \
    }

#define TEST_NO_VALID_DELETE(QUERY)                                                                                    \
    SECTION(QUERY) {                                                                                                   \
        auto query = QUERY;                                                                                            \
        auto res = sql::parse(resource, query);                                                                        \
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));                                               \
    }

#define TEST_ERROR_DELETE(QUERY, ERROR, TEXT, POS)                                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto query = QUERY;                                                                                            \
        auto res = sql::parse(resource, query);                                                                        \
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));                                               \
        REQUIRE(res.error.error() == ERROR);                                                                           \
        REQUIRE(res.error.mistake() == TEXT);                                                                          \
        REQUIRE(res.error.mistake().data() == query + POS);                                                            \
    }

using v = ::document::value_t;
using vec = std::vector<v>;

TEST_CASE("parser::delete_from_where") {
    auto* resource = std::pmr::get_default_resource();
    auto tape = std::make_unique<document::impl::mutable_document>(resource);
    auto new_value = [&](auto value) { return v{resource, tape.get(), value}; };

    TEST_SIMPLE_DELETE("delete from schema.table where number == 10;",
                       R"_($match: {"number": {$eq: #0}})_",
                       vec({new_value(10l)}));

    TEST_SIMPLE_DELETE(
        "delete from schema.table where not (number = 10) and not(name = 'doc 10' or count = 2);",
        R"_($match: {$and: [$not: ["number": {$eq: #0}], $not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]})_",
        vec({new_value(10l), new_value(std::pmr::string("doc 10")), new_value(2l)}));
}

TEST_CASE("parser::delete no valid queries") {
    auto* resource = std::pmr::get_default_resource();

    TEST_NO_VALID_DELETE("delete * from .table where number == 10;");
}

TEST_CASE("parser::delete errors") {
    auto* resource = std::pmr::get_default_resource();

    TEST_ERROR_DELETE("delete from schema.table where number == 10 group by name;",
                      sql::parse_error::syntax_error,
                      "group",
                      44);

    TEST_ERROR_DELETE("delete from schema.table where number == 10 order by name;",
                      sql::parse_error::syntax_error,
                      "order",
                      44);

    TEST_ERROR_DELETE("delete from schema. where number == 10;", sql::parse_error::syntax_error, " ", 19);

    TEST_ERROR_DELETE("delete from .table where number == 10;", sql::parse_error::syntax_error, ".", 12);

    TEST_ERROR_DELETE("delete from schema.table where number == 10 name = 'doc 10';",
                      sql::parse_error::syntax_error,
                      "name",
                      44);

    TEST_ERROR_DELETE("delete from schema.table where number == 10 and and = 'doc 10';",
                      sql::parse_error::syntax_error,
                      "and",
                      48);
}
