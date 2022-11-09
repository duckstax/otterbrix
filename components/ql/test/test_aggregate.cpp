#include <catch2/catch.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/aggregate/match.hpp>
#include <components/ql/aggregate/group.hpp>

using namespace components;
using namespace components::ql;
using namespace components::ql::aggregate;
using components::ql::experimental::make_project_expr;
using components::ql::experimental::project_expr_type;
using core::parameter_id_t;

TEST_CASE("aggregate::match") {
    auto match = make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1)));
    REQUIRE(debug(match) == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("aggregate::group") {
    {
        group_t group;
        auto expr = make_project_expr(project_expr_type::get_field, ql::key_t("_id"));
        expr->append_param(ql::key_t("date"));
        group.fields.push_back(std::move(expr));
        expr = make_project_expr(project_expr_type::sum, ql::key_t("total"));
        auto expr_multiply = make_project_expr(project_expr_type::multiply, ql::key_t());
        expr_multiply->append_param(ql::key_t("price"));
        expr_multiply->append_param(ql::key_t("quantity"));
        expr->append_param(std::move(expr_multiply));
        group.fields.push_back(std::move(expr));
        expr = make_project_expr(project_expr_type::avg, ql::key_t("avg_quantity"));
        expr->append_param(ql::key_t("quantity"));
        group.fields.push_back(std::move(expr));
        REQUIRE(debug(group) == R"_($group: {_id: "$date", total: {$sum: {$multiply: ["$price", "$quantity"]}}, avg_quantity: {$avg: "$quantity"}})_");
    }
    {
        group_t group;
        auto expr = make_project_expr(project_expr_type::get_field, ql::key_t("_id"));
        expr->append_param(ql::key_t("date"));
        group.fields.push_back(std::move(expr));
        expr = make_project_expr(project_expr_type::multiply, ql::key_t("count_4"));
        expr->append_param(parameter_id_t(1));
        expr->append_param(ql::key_t("count"));
        group.fields.push_back(std::move(expr));
        REQUIRE(debug(group) == R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}})_");
    }
}

TEST_CASE("aggregate") {
    aggregate_statement aggregate("database", "collection");
    aggregate.append(operator_type::match, make_match(experimental::make_expr(condition_type::eq, "key", parameter_id_t(1))));
    REQUIRE(debug(aggregate) == R"_($aggregate: {$match: {"key": {$eq: #1}}})_");
}
