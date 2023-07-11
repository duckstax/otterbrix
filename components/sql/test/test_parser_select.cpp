#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::select_base") {

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

}
