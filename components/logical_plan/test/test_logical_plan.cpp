#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_match.hpp>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

TEST_CASE("logical_plan::match") {
    auto match = components::ql::aggregate::make_match(make_compare_expression(compare_type::eq, key("key"), core::parameter_id_t(1)));
    auto node_math = make_node_match(match);
    REQUIRE(node_math->to_string() == R"_($match: {"key": {$eq: #1}})_");
}