#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/serialization/serializer.hpp>
#include <expressions/aggregate_expression.hpp>
#include <expressions/scalar_expression.hpp>
#include <logical_plan/node_delete.hpp>
#include <logical_plan/node_group.hpp>
#include <logical_plan/node_match.hpp>
#include <logical_plan/param_storage.hpp>

using namespace components::serializer;
using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() { return {database_name, collection_name}; }

TEST_CASE("json_serialization") {
    auto resource = std::pmr::synchronized_pool_resource();

    {
        json_serializer_t serializer(&resource);
        auto expr_and = make_compare_union_expression(&resource, compare_type::union_and);
        expr_and->append_child(
            make_compare_expression(&resource, compare_type::gt, key{"some key"}, core::parameter_id_t{1}));
        expr_and->append_child(
            make_compare_expression(&resource, compare_type::lt, key{"some other key"}, core::parameter_id_t{2}));
        expr_and->serialize(&serializer);
        auto res = serializer.result();
        REQUIRE(res == R"_(["compare_expression_t",10,null,null,0,[)_"
                       R"_(["compare_expression_t",3,"some key",null,1,[]],)_"
                       R"_(["compare_expression_t",4,"some other key",null,2,[]]]])_");
    }
    {
        json_serializer_t serializer(&resource);
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        expressions.emplace_back(std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(&resource, aggregate_type::sum, key("total"));
        auto expr_multiply = make_scalar_expression(&resource, scalar_type::multiply);
        expr_multiply->append_param(key("price"));
        expr_multiply->append_param(key("quantity"));
        agg_expr->append_param(std::move(expr_multiply));
        expressions.emplace_back(std::move(agg_expr));
        agg_expr = make_aggregate_expression(&resource, aggregate_type::avg, key("avg_quantity"));
        agg_expr->append_param(key("quantity"));
        expressions.emplace_back(std::move(agg_expr));
        auto node_group = make_node_group(&resource, get_name(), expressions);
        node_group->serialize(&serializer);
        auto res = serializer.result();
        REQUIRE(res == R"_(["node_group_t",["database","collection"],[)_"
                       R"_(["scalar_expression_t",1,"_id",[]],)_"
                       R"_(["aggregate_expression_t",2,"total",)_"
                       R"_([["scalar_expression_t",4,null,[]]]],)_"
                       R"_(["aggregate_expression_t",5,"avg_quantity",[]]]])_");
    }
    {
        json_serializer_t serializer(&resource);
        auto tape = std::make_unique<components::document::impl::base_document>(&resource);
        auto new_value = [&](auto value) { return components::document::value_t{tape.get(), value}; };
        auto node_delete = make_node_delete_many(
            &resource,
            {database_name, collection_name},
            make_node_match(
                &resource,
                {database_name, collection_name},
                make_compare_expression(&resource, compare_type::gt, key{"count"}, core::parameter_id_t{1})));
        auto params = make_parameter_node(&resource);
        params->add_parameter(core::parameter_id_t{1}, new_value(90));
        serializer.start_array(2);
        node_delete->serialize(&serializer);
        params->serialize(&serializer);
        serializer.end_array();
        auto res = serializer.result();
        REQUIRE(res == R"_([["node_delete_t",["database","collection"],[)_"
                       R"_(["node_match_t",["database","collection"],[)_"
                       R"_(["compare_expression_t",3,"count",null,1,[]]]],)_"
                       R"_(["node_limit_t",["database","collection"],-1]]],)_"
                       R"_(["parameter_node_t",[1,90]]])_");
    }
}