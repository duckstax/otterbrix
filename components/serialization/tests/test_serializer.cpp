#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

using namespace components::serializer;
using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() { return {database_name, collection_name}; }

TEST_CASE("serialization") {
    auto resource = std::pmr::synchronized_pool_resource();

    {
        auto expr_and = make_compare_union_expression(&resource, compare_type::union_and);
        expr_and->append_child(
            make_compare_expression(&resource, compare_type::gt, key{"some key"}, core::parameter_id_t{1}));
        expr_and->append_child(
            make_compare_expression(&resource, compare_type::lt, key{"some other key"}, core::parameter_id_t{2}));

        {
            json_serializer_t serializer(&resource);
            serializer.start_array(1);
            expr_and->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            REQUIRE(res == R"_([[17,10,null,null,0,[)_"
                           R"_([17,3,"some key",null,1,[]],)_"
                           R"_([17,4,"some other key",null,2,[]]]]])_");
            json_deserializer_t deserializer(res);
            auto deserialized_res = deserializer.deserialize(0);
            REQUIRE(std::holds_alternative<expression_ptr>(deserialized_res));
            REQUIRE(expr_and->to_string() == std::get<expression_ptr>(deserialized_res)->to_string());
        }
        {
            msgpack_serializer_t serializer(&resource);
            serializer.start_array(1);
            expr_and->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            // res is not stored in a readable format
            msgpack_deserializer_t deserializer(res);
            auto deserialized_res = deserializer.deserialize(0);
            REQUIRE(std::holds_alternative<expression_ptr>(deserialized_res));
            REQUIRE(expr_and->to_string() == std::get<expression_ptr>(deserialized_res)->to_string());
        }
    }
    {
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
        {
            json_serializer_t serializer(&resource);
            serializer.start_array(1);
            node_group->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            REQUIRE(res == R"_([[13,["database","collection"],[)_"
                           R"_([19,1,"_id",["date"]],)_"
                           R"_([18,2,"total",)_"
                           R"_([[19,4,null,["price","quantity"]]]],)_"
                           R"_([18,5,"avg_quantity",["quantity"]]]]])_");
            json_deserializer_t deserializer(res);
            auto deserialized_res = deserializer.deserialize(0);
            REQUIRE(std::holds_alternative<node_ptr>(deserialized_res));
            REQUIRE(node_group->to_string() == std::get<node_ptr>(deserialized_res)->to_string());
        }
        {
            msgpack_serializer_t serializer(&resource);
            serializer.start_array(1);
            node_group->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            msgpack_deserializer_t deserializer(res);
            auto deserialized_res = deserializer.deserialize(0);
            REQUIRE(std::holds_alternative<node_ptr>(deserialized_res));
            REQUIRE(node_group->to_string() == std::get<node_ptr>(deserialized_res)->to_string());
        }
    }
    {
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
        {
            json_serializer_t serializer(&resource);
            serializer.start_array(2);
            node_delete->serialize(&serializer);
            params->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            REQUIRE(res == R"_([[5,["database","collection"],)_"
                           R"_([[12,["database","collection"],)_"
                           R"_([17,3,"count",null,1,[]]],)_"
                           R"_([11,["database","collection"],-1]]],)_"
                           R"_([21,[[1,90]]]])_");
            json_deserializer_t deserializer(res);
            auto deserialized_res = deserializer.deserialize(0);
            REQUIRE(std::holds_alternative<node_ptr>(deserialized_res));
            REQUIRE(node_delete->to_string() == std::get<node_ptr>(deserialized_res)->to_string());
            deserialized_res = deserializer.deserialize(1);
            REQUIRE(std::holds_alternative<parameter_node_ptr>(deserialized_res));
            REQUIRE(params->parameters().parameters ==
                    std::get<parameter_node_ptr>(deserialized_res)->parameters().parameters);
        }
        {
            msgpack_serializer_t serializer(&resource);
            serializer.start_array(2);
            node_delete->serialize(&serializer);
            params->serialize(&serializer);
            serializer.end_array();
            auto res = serializer.result();
            msgpack_deserializer_t deserializer(res);
            auto deserialized_res = deserializer.deserialize(0);
            REQUIRE(std::holds_alternative<node_ptr>(deserialized_res));
            REQUIRE(node_delete->to_string() == std::get<node_ptr>(deserialized_res)->to_string());
            deserialized_res = deserializer.deserialize(1);
            REQUIRE(std::holds_alternative<parameter_node_ptr>(deserialized_res));
            REQUIRE(params->parameters().parameters ==
                    std::get<parameter_node_ptr>(deserialized_res)->parameters().parameters);
        }
    }
}