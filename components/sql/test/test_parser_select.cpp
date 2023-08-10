#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

#define TEST_SIMPLE_SELECT(QUERY, RESULT, PARAMS)                        \
    SECTION(QUERY) {                                                     \
        auto res = sql::parse(resource, QUERY);                          \
        auto ql = res.ql;                                                \
        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));    \
        auto& agg = std::get<ql::aggregate_statement>(ql);               \
        REQUIRE(agg.database_ == "schema");                              \
        REQUIRE(agg.collection_ == "table");                             \
        std::stringstream s;                                             \
        s << agg;                                                        \
        REQUIRE(s.str() == RESULT);                                      \
        REQUIRE(agg.parameters().size() == PARAMS.size());               \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                     \
            REQUIRE(agg.parameter(core::parameter_id_t(uint16_t(i)))     \
                    == PARAMS.at(i));                                    \
        }                                                                \
    }

#define TEST_SELECT_WITHOUT_FROM(QUERY, RESULT, PARAMS)                  \
    SECTION(QUERY) {                                                     \
        auto res = sql::parse(resource, QUERY);                          \
        auto ql = res.ql;                                                \
        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));    \
        auto& agg = std::get<ql::aggregate_statement>(ql);               \
        std::stringstream s;                                             \
        s << agg;                                                        \
        REQUIRE(s.str() == RESULT);                                      \
        REQUIRE(agg.parameters().size() == PARAMS.size());               \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                     \
            REQUIRE(agg.parameter(core::parameter_id_t(uint16_t(i)))     \
                    == PARAMS.at(i));                                    \
        }                                                                \
    }

#define TEST_ERROR_SELECT(QUERY, ERROR, TEXT, POS)                       \
    SECTION(QUERY) {                                                     \
        auto query = QUERY;                                              \
        auto res = sql::parse(resource, query);                          \
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql)); \
        REQUIRE(res.error.error() == ERROR);                             \
        REQUIRE(res.error.mistake() == TEXT);                            \
        REQUIRE(res.error.mistake().data() == query + POS);              \
    }

using v = ::document::wrapper_value_t;
using vec = std::vector<v>;

TEST_CASE("parser::select_from_where") {

    auto* resource = std::pmr::get_default_resource();

    SECTION("select * from schema.table;") {
        auto query = "select * from schema.table;";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 0);
    }

    SECTION("select * from schema.table where number = 10;") {
        auto query = "select * from schema.table where number = 10;";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 1);

        std::stringstream s;
        REQUIRE(agg.type_operator(0) == ql::aggregate::operator_type::match);
        s << std::get<ql::aggregate_statement>(ql).get_operator<ql::aggregate::match_t>(0);
        REQUIRE(s.str() == R"_($match: {"number": {$eq: #0}})_");

        REQUIRE(agg.parameters().size() == 1);
        REQUIRE(agg.parameter(core::parameter_id_t{0})->as_int() == 10);
    }

    SECTION("select * from schema.table where number = 10 and name = 'doc 10' and count = 2;") {
        auto query = "select * from schema.table where number = 10 and name = 'doc 10' and count = 2;";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 1);

        std::stringstream s;
        REQUIRE(agg.type_operator(0) == ql::aggregate::operator_type::match);
        s << std::get<ql::aggregate_statement>(ql).get_operator<ql::aggregate::match_t>(0);
        REQUIRE(s.str() == R"_($match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]})_");

        REQUIRE(agg.parameters().size() == 3);
        REQUIRE(agg.parameter(core::parameter_id_t{0})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{1})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{2})->as_int() == 2);
    }

    SECTION("select * from schema.table where number = 10 or name = 'doc 10' or count = 2;") {
        auto query = "select * from schema.table where number = 10 or name = 'doc 10' or count = 2;";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 1);

        std::stringstream s;
        REQUIRE(agg.type_operator(0) == ql::aggregate::operator_type::match);
        s << std::get<ql::aggregate_statement>(ql).get_operator<ql::aggregate::match_t>(0);
        REQUIRE(s.str() == R"_($match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]})_");

        REQUIRE(agg.parameters().size() == 3);
        REQUIRE(agg.parameter(core::parameter_id_t{0})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{1})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{2})->as_int() == 2);
    }

    SECTION("select * from schema.table where number = 10 and name = 'doc 10' or count = 2;") {
        auto query = "select * from schema.table where number = 10 and name = 'doc 10' or count = 2;";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 1);

        std::stringstream s;
        REQUIRE(agg.type_operator(0) == ql::aggregate::operator_type::match);
        s << std::get<ql::aggregate_statement>(ql).get_operator<ql::aggregate::match_t>(0);
        REQUIRE(s.str() == R"_($match: {$and: ["number": {$eq: #0}, $or: ["name": {$eq: #1}, "count": {$eq: #2}]]})_");

        REQUIRE(agg.parameters().size() == 3);
        REQUIRE(agg.parameter(core::parameter_id_t{0})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{1})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{2})->as_int() == 2);
    }

    SECTION("select * from schema.table where (number = 10 and name = 'doc 10') or count = 2;") {
        auto query = "select * from schema.table where (number = 10 and name = 'doc 10') or count = 2;";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 1);

        std::stringstream s;
        REQUIRE(agg.type_operator(0) == ql::aggregate::operator_type::match);
        s << std::get<ql::aggregate_statement>(ql).get_operator<ql::aggregate::match_t>(0);
        REQUIRE(s.str() == R"_($match: {$or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}]})_");

        REQUIRE(agg.parameters().size() == 3);
        REQUIRE(agg.parameter(core::parameter_id_t{0})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{1})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{2})->as_int() == 2);
    }

    SECTION("select * from schema.table where ((number = 10 and name = 'doc 10') or count = 2) and "
            "((number = 10 and name = 'doc 10') or count = 2) and "
            "((number = 10 and name = 'doc 10') or count = 2);") {
        auto query = "select * from schema.table where ((number = 10 and name = 'doc 10') or count = 2) and "
                     "((number = 10 and name = 'doc 10') or count = 2) and "
                     "((number = 10 and name = 'doc 10') or count = 2);";
        auto res = sql::parse(resource, query);
        auto ql = res.ql;

        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));

        auto& agg = std::get<ql::aggregate_statement>(ql);
        REQUIRE(agg.database_ == "schema");
        REQUIRE(agg.collection_ == "table");
        REQUIRE(agg.count_operators() == 1);

        std::stringstream s;
        REQUIRE(agg.type_operator(0) == ql::aggregate::operator_type::match);
        s << std::get<ql::aggregate_statement>(ql).get_operator<ql::aggregate::match_t>(0);
        REQUIRE(s.str() == R"_($match: {$and: [)_"
                           R"_($or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}], )_"
                           R"_($or: [$and: ["number": {$eq: #3}, "name": {$eq: #4}], "count": {$eq: #5}], )_"
                           R"_($or: [$and: ["number": {$eq: #6}, "name": {$eq: #7}], "count": {$eq: #8}])_"
                           R"_(]})_");

        REQUIRE(agg.parameters().size() == 9);
        REQUIRE(agg.parameter(core::parameter_id_t{0})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{1})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{2})->as_int() == 2);
        REQUIRE(agg.parameter(core::parameter_id_t{3})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{4})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{5})->as_int() == 2);
        REQUIRE(agg.parameter(core::parameter_id_t{6})->as_int() == 10);
        REQUIRE(agg.parameter(core::parameter_id_t{7})->as_string() == "doc 10");
        REQUIRE(agg.parameter(core::parameter_id_t{8})->as_int() == 2);
    }

    TEST_SIMPLE_SELECT("select * from schema.table where number == 10;",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where number != 10;",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where number <> 10;",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where number < 10;",
                       R"_($aggregate: {$match: {"number": {$lt: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where number <= 10;",
                       R"_($aggregate: {$match: {"number": {$lte: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where number > 10;",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where number >= 10;",
                       R"_($aggregate: {$match: {"number": {$gte: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where not(number >= 10);",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where not number >= 10;",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where not (number = 10) and not(name = 'doc 10' or count = 2);",
                       R"_($aggregate: {$match: {$and: [$not: ["number": {$eq: #0}], )_"
                       R"_($not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT("select * from schema.table where name regexp 'pattern';",
                       R"_($aggregate: {$match: {"name": {$regex: #0}}})_",
                       vec({v(std::string("pattern"))}));

}

TEST_CASE("parser::select_from_order_by") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SIMPLE_SELECT("select * from schema.table order by number;",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT("select * from schema.table order by number asc;",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT("select * from schema.table order by number desc;",
                       R"_($aggregate: {$sort: {number: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT("select * from schema.table order by number, name;",
                       R"_($aggregate: {$sort: {number: 1, name: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT("select * from schema.table order by number asc, name desc;",
                       R"_($aggregate: {$sort: {number: 1, name: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT("select * from schema.table order by number, count asc, name, value desc;",
                       R"_($aggregate: {$sort: {number: 1, count: 1, name: -1, value: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT("select * from schema.table where number > 10 order by number asc, name desc;",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}, $sort: {number: 1, name: -1}})_",
                       vec({v(10l)}));

}

TEST_CASE("parser::select_from_fields") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SIMPLE_SELECT("select number, name, count from schema.table;",
                       R"_($aggregate: {$group: {number, name, count}})_",
                       vec());

    TEST_SIMPLE_SELECT("select number, name as title from schema.table;",
                       R"_($aggregate: {$group: {number, title: "$name"}})_",
                       vec());

    TEST_SIMPLE_SELECT("select number, name title from schema.table;",
                       R"_($aggregate: {$group: {number, title: "$name"}})_",
                       vec());

    TEST_SIMPLE_SELECT("select number, 10 size, 'title' title, true on, false off from schema.table;",
                       R"_($aggregate: {$group: {number, size: #0, title: #1, on: #2, off: #3}})_",
                       vec({v(10l), v(std::string("title")), v(true), v(false)}));

}

TEST_CASE("parser::select_from_fields::errors") {

    auto* resource = std::pmr::get_default_resource();

    TEST_ERROR_SELECT("select number name count from schema.table;",
                      sql::parse_error::syntax_error, "count", 19);

    TEST_ERROR_SELECT("select number as, name, count from schema.table;",
                      sql::parse_error::syntax_error, ",", 16);

}

TEST_CASE("parser::select_without_from") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SELECT_WITHOUT_FROM("select 10 number;",
                             R"_($aggregate: {$group: {number: #0}})_",
                             vec({v(10l)}));

    TEST_SELECT_WITHOUT_FROM("select 'title' as title;",
                             R"_($aggregate: {$group: {title: #0}})_",
                             vec({v(std::string("title"))}));

    TEST_SELECT_WITHOUT_FROM("select 10;",
                             R"_($aggregate: {$group: {10: #0}})_",
                             vec({v(10l)}));

    TEST_SELECT_WITHOUT_FROM("select 'title';",
                             R"_($aggregate: {$group: {title: #0}})_",
                             vec({v(std::string("title"))}));

    TEST_SELECT_WITHOUT_FROM("select 10, 'title';",
                             R"_($aggregate: {$group: {10: #0, title: #1}})_",
                             vec({v(10l), v(std::string("title"))}));

}
