#include <actor-zeta.hpp>
#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/aggregate/group.hpp>
#include <components/ql/aggregate/match.hpp>
#include <components/ql/aggregate/sort.hpp>
#include <sstream>

using namespace components;
using namespace components::ql;
using namespace components::ql::aggregate;
using components::expressions::aggregate_type;
using components::expressions::compare_type;
using components::expressions::make_aggregate_expression;
using components::expressions::make_compare_expression;
using components::expressions::make_scalar_expression;
using components::expressions::scalar_type;
using components::expressions::sort_order;
using key = components::expressions::key_t;
using core::parameter_id_t;

template<class T>
std::string debug(const T& value) {
    std::stringstream stream;
    stream << value;
    return stream.str();
}

TEST_CASE("aggregate::match") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto match = make_match(make_compare_expression(resource, compare_type::eq, key("key"), parameter_id_t(1)));
    REQUIRE(debug(match) == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("aggregate::group") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    {
        group_t group;
        auto scalar_expr = make_scalar_expression(resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(resource, aggregate_type::sum, key("total"));
        auto expr_multiply = make_scalar_expression(resource, scalar_type::multiply);
        expr_multiply->append_param(key("price"));
        expr_multiply->append_param(key("quantity"));
        agg_expr->append_param(std::move(expr_multiply));
        append_expr(group, std::move(agg_expr));
        agg_expr = make_aggregate_expression(resource, aggregate_type::avg, key("avg_quantity"));
        agg_expr->append_param(key("quantity"));
        append_expr(group, std::move(agg_expr));
        REQUIRE(
            debug(group) ==
            R"_($group: {_id: "$date", total: {$sum: {$multiply: ["$price", "$quantity"]}}, avg_quantity: {$avg: "$quantity"}})_");
    }
    {
        group_t group;
        auto scalar_expr = make_scalar_expression(resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        scalar_expr = make_scalar_expression(resource, scalar_type::multiply, key("count_4"));
        scalar_expr->append_param(parameter_id_t(1));
        scalar_expr->append_param(key("count"));
        append_expr(group, std::move(scalar_expr));
        REQUIRE(debug(group) == R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}})_");
    }
}

TEST_CASE("aggregate::sort") {
    sort_t sort;
    append_sort(sort, key("name"), sort_order::asc);
    append_sort(sort, key("count"), sort_order::desc);
    append_sort(sort, key("_id"), sort_order::asc);
    REQUIRE(debug(sort) == R"_($sort: {name: 1, count: -1, _id: 1})_");
}

TEST_CASE("aggregate") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    SECTION("aggregate::only_match") {
        aggregate_statement aggregate("database", "collection");
        aggregate.append(
            operator_type::match,
            make_match(make_compare_expression(resource, compare_type::eq, key("key"), parameter_id_t(1))));
        REQUIRE(debug(aggregate) == R"_($aggregate: {$match: {"key": {$eq: #1}}})_");
    }
    SECTION("aggregate::only_group") {
        aggregate_statement aggregate("database", "collection");
        group_t group;
        auto scalar_expr = make_scalar_expression(resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        scalar_expr = make_scalar_expression(resource, scalar_type::multiply, key("count_4"));
        scalar_expr->append_param(parameter_id_t(1));
        scalar_expr->append_param(key("count"));
        append_expr(group, std::move(scalar_expr));
        aggregate.append(operator_type::group, std::move(group));
        REQUIRE(debug(aggregate) == R"_($aggregate: {$group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}}})_");
    }
    SECTION("aggregate::only_sort") {
        aggregate_statement aggregate("database", "collection");
        sort_t sort;
        append_sort(sort, key("name"), sort_order::asc);
        append_sort(sort, key("count"), sort_order::desc);
        aggregate.append(operator_type::sort, std::move(sort));
        REQUIRE(debug(aggregate) == R"_($aggregate: {$sort: {name: 1, count: -1}})_");
    }
    SECTION("aggregate::all") {
        aggregate_statement aggregate("database", "collection");

        aggregate.append(
            operator_type::match,
            make_match(make_compare_expression(resource, compare_type::eq, key("key"), parameter_id_t(1))));

        group_t group;
        auto scalar_expr = make_scalar_expression(resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        scalar_expr = make_scalar_expression(resource, scalar_type::multiply, key("count_4"));
        scalar_expr->append_param(parameter_id_t(1));
        scalar_expr->append_param(key("count"));
        append_expr(group, std::move(scalar_expr));
        aggregate.append(operator_type::group, std::move(group));

        sort_t sort;
        append_sort(sort, key("name"), sort_order::asc);
        append_sort(sort, key("count"), sort_order::desc);
        aggregate.append(operator_type::sort, std::move(sort));

        REQUIRE(debug(aggregate) == R"_($aggregate: {)_"
                                    R"_($match: {"key": {$eq: #1}}, )_"
                                    R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}}, )_"
                                    R"_($sort: {name: 1, count: -1})_"
                                    R"_(})_");
    }
}
