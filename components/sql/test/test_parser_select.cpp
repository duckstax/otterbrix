#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

#define TEST_SIMPLE_SELECT(QUERY, RESULT)                                       \
    SECTION(QUERY) {                                                            \
        auto res = sql::parse(resource, QUERY);                                 \
        auto ql = res.ql;                                                       \
        REQUIRE(std::holds_alternative<ql::aggregate_statement>(ql));           \
        auto& agg = std::get<ql::aggregate_statement>(ql);                      \
        REQUIRE(agg.database_ == "schema");                                     \
        REQUIRE(agg.collection_ == "table");                                    \
        std::stringstream s;                                                    \
        s << agg;                                                               \
        REQUIRE(s.str() == RESULT);                                             \
    }

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
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where number != 10;",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where number <> 10;",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where number < 10;",
                       R"_($aggregate: {$match: {"number": {$lt: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where number <= 10;",
                       R"_($aggregate: {$match: {"number": {$lte: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where number > 10;",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where number >= 10;",
                       R"_($aggregate: {$match: {"number": {$gte: #0}}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where not(number >= 10);",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where not number >= 10;",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_");

    TEST_SIMPLE_SELECT("select * from schema.table where not (number = 10) and not(name = 'doc 10' or count = 2);",
                       R"_($aggregate: {$match: {$and: [$not: ["number": {$eq: #0}], )_"
                       R"_($not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}})_");

}
