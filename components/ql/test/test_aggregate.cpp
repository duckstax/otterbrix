#include <catch2/catch.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/aggregate/match.hpp>
#include <components/ql/aggregate/group.hpp>

using namespace components::ql;
using namespace components::ql::aggregate;
using core::parameter_id_t;

TEST_CASE("aggregate::match") {
    auto match = make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1)));
    REQUIRE(debug(match) == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("aggregate::group") {
//    auto match = make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1)));
//    REQUIRE(debug(match) == R"_($group: {_id: "$date", total: {$sum: {$multiply: ["$price", "$quantity"]}}, avg_quantity: {$avg: "$quantity"}})_");
//    REQUIRE(debug(match) == R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}}})_");
}

TEST_CASE("aggregate") {
    aggregate_statement aggregate("database", "collection");
    aggregate.append(operator_type::match, make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1))));
    REQUIRE(debug(aggregate) == R"_($aggregate: {$match: {"key": {$eq: #1}}})_");
}
