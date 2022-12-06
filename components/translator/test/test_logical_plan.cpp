#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/ql/aggregate.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/translator/ql_translator.hpp>

using namespace components::translator;
using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

TEST_CASE("logical_plan::match") {
    auto match = components::ql::aggregate::make_match(make_compare_expression(compare_type::eq, key("key"), core::parameter_id_t(1)));
    auto node_match = make_node_match(match);
    REQUIRE(node_match->to_string() == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("logical_plan::group") {
    {
        components::ql::aggregate::group_t group;
        auto scalar_expr = make_scalar_expression(scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(aggregate_type::sum, key("total"));
        auto expr_multiply = make_scalar_expression(scalar_type::multiply);
        expr_multiply->append_param(key("price"));
        expr_multiply->append_param(key("quantity"));
        agg_expr->append_param(std::move(expr_multiply));
        append_expr(group, std::move(agg_expr));
        agg_expr = make_aggregate_expression(aggregate_type::avg, key("avg_quantity"));
        agg_expr->append_param(key("quantity"));
        append_expr(group, std::move(agg_expr));
        auto node_group = make_node_group(group);
        REQUIRE(node_group->to_string() == R"_($group: {_id: "$date", total: {$sum: {$multiply: ["$price", "$quantity"]}}, avg_quantity: {$avg: "$quantity"}})_");
    }
    {
        components::ql::aggregate::group_t group;
        auto scalar_expr = make_scalar_expression(scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        scalar_expr = make_scalar_expression(scalar_type::multiply, key("count_4"));
        scalar_expr->append_param(core::parameter_id_t(1));
        scalar_expr->append_param(key("count"));
        append_expr(group, std::move(scalar_expr));
        auto node_group = make_node_group(group);
        REQUIRE(node_group->to_string() == R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}})_");
    }
}

TEST_CASE("logical_plan::sort") {
    {
        components::ql::aggregate::sort_t sort;
        components::ql::aggregate::append_sort(sort, key("key"), sort_order::asc);
        auto node_sort = make_node_sort(sort);
        REQUIRE(node_sort->to_string() == R"_($sort: {key: 1})_");
    }
    {
        components::ql::aggregate::sort_t sort;
        components::ql::aggregate::append_sort(sort, key("key1"), sort_order::asc);
        components::ql::aggregate::append_sort(sort, key("key2"), sort_order::desc);
        auto node_sort = make_node_sort(sort);
        REQUIRE(node_sort->to_string() == R"_($sort: {key1: 1, key2: -1})_");
    }
}

TEST_CASE("logical_plan::aggregate") {
    using components::ql::aggregate::operator_type;

    components::ql::aggregate_statement aggregate("database", "collection");

    aggregate.append(operator_type::match, components::ql::aggregate::make_match(make_compare_expression(compare_type::eq, key("key"), core::parameter_id_t(1))));

    components::ql::aggregate::group_t group;
    auto scalar_expr = make_scalar_expression(scalar_type::get_field, key("_id"));
    scalar_expr->append_param(key("date"));
    append_expr(group, std::move(scalar_expr));
    scalar_expr = make_scalar_expression(scalar_type::multiply, key("count_4"));
    scalar_expr->append_param(core::parameter_id_t(1));
    scalar_expr->append_param(key("count"));
    append_expr(group, std::move(scalar_expr));
    aggregate.append(operator_type::group, std::move(group));

    components::ql::aggregate::sort_t sort;
    append_sort(sort, key("name"), sort_order::asc);
    append_sort(sort, key("count"), sort_order::desc);
    aggregate.append(operator_type::sort, std::move(sort));

    auto node_aggregate = ql_translator(&aggregate);

    REQUIRE(node_aggregate->to_string() == R"_($aggregate: {)_"
                                           R"_($match: {"key": {$eq: #1}}, )_"
                                           R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}}, )_"
                                           R"_($sort: {name: 1, count: -1})_"
                                           R"_(})_");
}
