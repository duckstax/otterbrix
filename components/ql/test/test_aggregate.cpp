#include <catch2/catch.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/aggregate/match.hpp>

using namespace components::ql;
using namespace components::ql::aggregate;
using core::parameter_id_t;

TEST_CASE("aggregate::match") {
    auto match = make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1)));
    REQUIRE(debug(match) == R"($match: {"key": {$eq: #1}})");
}

TEST_CASE("aggregate") {
    aggregate_statement aggregate("database", "collection");
    aggregate.append(operator_type::match, make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1))));
    REQUIRE(debug(aggregate) == R"($aggregate: {$match: {"key": {$eq: #1}}})");
}
