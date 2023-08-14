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

    TEST_SIMPLE_SELECT(R"_(select * from schema.table;)_",
                       R"_($aggregate: {})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number = 10;)_",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number = 10 and name = 'doc 10' and "count" = 2;)_",
                       R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number = 10 or name = 'doc 10' or "count" = 2;)_",
                       R"_($aggregate: {$match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number = 10 and name = 'doc 10' or "count" = 2;)_",
                       R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, $or: ["name": {$eq: #1}, "count": {$eq: #2}]]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where (number = 10 and name = 'doc 10') or "count" = 2;)_",
                       R"_($aggregate: {$match: {$or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where ((number = 10 and name = 'doc 10') or "count" = 2) and )_"
                       R"_(((number = 10 and name = 'doc 10') or "count" = 2) and )_"
                       R"_(((number = 10 and name = 'doc 10') or "count" = 2);)_",
                       R"_($aggregate: {$match: {$and: [)_"
                       R"_($or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}], )_"
                       R"_($or: [$and: ["number": {$eq: #3}, "name": {$eq: #4}], "count": {$eq: #5}], )_"
                       R"_($or: [$and: ["number": {$eq: #6}, "name": {$eq: #7}], "count": {$eq: #8}])_"
                       R"_(]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l),
                            v(10l), v(std::string("doc 10")), v(2l),
                            v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number == 10;)_",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number != 10;)_",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number <> 10;)_",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number < 10;)_",
                       R"_($aggregate: {$match: {"number": {$lt: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number <= 10;)_",
                       R"_($aggregate: {$match: {"number": {$lte: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number > 10;)_",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number >= 10;)_",
                       R"_($aggregate: {$match: {"number": {$gte: #0}}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where not(number >= 10);)_",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where not number >= 10;)_",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({v(10l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where not (number = 10) and not(name = 'doc 10' or "count" = 2);)_",
                       R"_($aggregate: {$match: {$and: [$not: ["number": {$eq: #0}], )_"
                       R"_($not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}})_",
                       vec({v(10l), v(std::string("doc 10")), v(2l)}));

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where name regexp 'pattern';)_",
                       R"_($aggregate: {$match: {"name": {$regex: #0}}})_",
                       vec({v(std::string("pattern"))}));

}

TEST_CASE("parser::select_from_order_by") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SIMPLE_SELECT(R"_(select * from schema.table order by number;)_",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table order by number asc;)_",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table order by number desc;)_",
                       R"_($aggregate: {$sort: {number: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table order by number, name;)_",
                       R"_($aggregate: {$sort: {number: 1, name: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table order by number asc, name desc;)_",
                       R"_($aggregate: {$sort: {number: 1, name: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table order by number, "count" asc, name, value desc;)_",
                       R"_($aggregate: {$sort: {number: 1, count: 1, name: -1, value: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select * from schema.table where number > 10 order by number asc, name desc;)_",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}, $sort: {number: 1, name: -1}})_",
                       vec({v(10l)}));

}

TEST_CASE("parser::select_from_fields") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SIMPLE_SELECT(R"_(select number, name, "count" from schema.table;)_",
                       R"_($aggregate: {$group: {number, name, count}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select number, name as title from schema.table;)_",
                       R"_($aggregate: {$group: {number, title: "$name"}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select number, name title from schema.table;)_",
                       R"_($aggregate: {$group: {number, title: "$name"}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(select number, 10 size, 'title' title, true on, false off from schema.table;)_",
                       R"_($aggregate: {$group: {number, size: #0, title: #1, on: #2, off: #3}})_",
                       vec({v(10l), v(std::string("title")), v(true), v(false)}));

}

TEST_CASE("parser::select_from_fields::errors") {

    auto* resource = std::pmr::get_default_resource();

    TEST_ERROR_SELECT(R"_(select number name "count" from schema.table;)_",
                      sql::parse_error::syntax_error, R"_("count")_", 19);

    TEST_ERROR_SELECT(R"_(select number as, name, "count" from schema.table;)_",
                      sql::parse_error::syntax_error, ",", 16);

}

TEST_CASE("parser::select_without_from") {

    auto* resource = std::pmr::get_default_resource();

    TEST_SELECT_WITHOUT_FROM(R"_(select 10 number;)_",
                             R"_($aggregate: {$group: {number: #0}})_",
                             vec({v(10l)}));

    TEST_SELECT_WITHOUT_FROM(R"_(select 'title' as title;)_",
                             R"_($aggregate: {$group: {title: #0}})_",
                             vec({v(std::string("title"))}));

    TEST_SELECT_WITHOUT_FROM(R"_(select 10;)_",
                             R"_($aggregate: {$group: {10: #0}})_",
                             vec({v(10l)}));

    TEST_SELECT_WITHOUT_FROM(R"_(select 'title';)_",
                             R"_($aggregate: {$group: {title: #0}})_",
                             vec({v(std::string("title"))}));

    TEST_SELECT_WITHOUT_FROM(R"_(select 10, 'title';)_",
                             R"_($aggregate: {$group: {10: #0, title: #1}})_",
                             vec({v(10l), v(std::string("title"))}));

}

TEST_CASE("parser::select_from_group_by") {

    auto* resource = std::pmr::get_default_resource();

    //TEST_SIMPLE_SELECT(R"_(select name, sum(count) as "count" from schema.table group by name;)_",
    TEST_SIMPLE_SELECT(R"_(select name, sum(count) as "count" from schema.table;)_",
                       R"_($aggregate: {$group: {name, count: {$sum: "$count"}}})_",
                       vec());

    //TEST_SIMPLE_SELECT(R"_(select name, sum(count) "count" from schema.table group by name;)_",
    TEST_SIMPLE_SELECT(R"_(select name, sum(count) "count" from schema.table;)_",
                       R"_($aggregate: {$group: {name, count: {$sum: "$count"}}})_",
                       vec());

    //TEST_SIMPLE_SELECT(R"_(select name, sum(count) from schema.table group by name;)_",
    TEST_SIMPLE_SELECT(R"_(select name, sum(count) from schema.table;)_",
                       R"_($aggregate: {$group: {name, sum(count): {$sum: "$count"}}})_",
                       vec());

}

TEST_CASE("parser::select_from_group_by::errors") {

//    auto* resource = std::pmr::get_default_resource();

//    TEST_ERROR_SELECT(R"_(select name, sum(count) as "count" from schema.table;)_",
//                      sql::parse_error::group_by_less_paramaters, "name", 19);

//    TEST_ERROR_SELECT(R"_(select name, sum(count) as "count" from schema.table group by name, title;)_",
//                      sql::parse_error::group_by_more_paramaters, "title", 19);

}
