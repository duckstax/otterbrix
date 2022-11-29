#include <catch2/catch.hpp>
#include <components/expressions/scalar_expression.hpp>

using namespace components::expressions;
using key = components::expressions::key_t;

TEST_CASE("expression::scalar::equals") {
    auto expr1 = make_scalar_expression(scalar_type::get_field, key("name"));
    auto expr2 = make_scalar_expression(scalar_type::get_field, key("name"));
    auto expr3 = make_scalar_expression(scalar_type::abs, key("name"));
    auto expr4 = make_scalar_expression(scalar_type::get_field, key("count"));
    auto expr_union1 = make_scalar_expression(scalar_type::multiply);
    expr_union1->append_param(core::parameter_id_t(0));
    expr_union1->append_param(key("name"));
    expr_union1->append_param(expr1);
    auto expr_union2 = make_scalar_expression(scalar_type::multiply);
    expr_union2->append_param(core::parameter_id_t(0));
    expr_union2->append_param(key("name"));
    expr_union2->append_param(expr1);
    auto expr_union3 = make_scalar_expression(scalar_type::multiply);
    expr_union3->append_param(core::parameter_id_t(1));
    expr_union3->append_param(key("name"));
    expr_union3->append_param(expr1);
    auto expr_union4 = make_scalar_expression(scalar_type::multiply);
    expr_union4->append_param(core::parameter_id_t(0));
    expr_union4->append_param(key("count"));
    expr_union4->append_param(expr1);
    auto expr_union5 = make_scalar_expression(scalar_type::multiply);
    expr_union5->append_param(core::parameter_id_t(0));
    expr_union5->append_param(key("name"));
    expr_union5->append_param(expr3);
    REQUIRE(expression_equal()(expr1, expr2));
    REQUIRE_FALSE(expression_equal()(expr1, expr3));
    REQUIRE_FALSE(expression_equal()(expr1, expr4));
    REQUIRE(expression_equal()(expr_union1, expr_union2));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union3));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union4));
    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union5));
}

TEST_CASE("expression::scalar::to_string") {
    auto expr = make_scalar_expression(scalar_type::get_field, key("count"), key("count"));
    REQUIRE(expr->to_string() == R"(count: "$count")");
    expr = make_scalar_expression(scalar_type::floor, key("count"), key("count"));
    REQUIRE(expr->to_string() == R"(count: {$floor: "$count"})");

    expr = make_scalar_expression(scalar_type::multiply, key("multi"));
    expr->append_param(core::parameter_id_t(1));
    expr->append_param(key("key"));
    expr->append_param(make_scalar_expression(scalar_type::get_field, key("value"), key("count")));
    REQUIRE(expr->to_string() == R"(multi: {$multiply: [#1, "$key", {value: "$count"}]})");
}

TEST_CASE("expression::scalar::parser") {
//    std::string value = R"({"count": {"$gt": 10}})";
//    auto d = components::document::document_from_json(value);
//    auto condition = parse_find_condition(d);
//    REQUIRE(to_string(condition) == value);
//
//    value = R"({"$or": [{"count": {"$gt": 10}}, {"count": {"$lte": 50}}]})";
//    d = components::document::document_from_json(value);
//    condition = parse_find_condition(d);
//    REQUIRE(to_string(condition) == value);
//
//    value = R"({"$or": [{"count": {"$gt": 10}}, {"$and": [{"count": {"$lte": 50}}, {"value": {"$gt": 3}}]}]})";
//    d = components::document::document_from_json(value);
//    condition = parse_find_condition(d);
//    REQUIRE(to_string(condition) == value);
//
//    value = R"({"$or": [{"count": {"$gt": 10}}, {"$and": [{"count": {"$lte": 50}}, {"value": {"$gt": 3}}, )"
//            R"({"$or": [{"count": {"$gt": 10}}, {"$and": [{"count": {"$lte": 50}}, {"value": {"$gt": 3}}]}]}]}]})";
//    d = components::document::document_from_json(value);
//    condition = parse_find_condition(d);
//    REQUIRE(to_string(condition) == value);
//
//    value = R"({"count": 10})";
//    d = components::document::document_from_json(value);
//    condition = parse_find_condition(d);
//    REQUIRE(to_string(condition) == R"({"count": {"$eq": 10}})");
//
//    value = R"({"count": {"$gt": 40, "$lte": 60}})";
//    d = components::document::document_from_json(value);
//    condition = parse_find_condition(d);
//    REQUIRE(to_string(condition) == R"({"$and": [{"count": {"$gt": 40}}, {"count": {"$lte": 60}}]})");
//
//    //    value = R"({"count": {"$not": {"$gte": 90, "$lt": 10}}})";
//    //    d = components::document::document_from_json(value);
//    //    condition = parse_find_condition(d);
//    //    REQUIRE(to_string(condition) == R"({"$not": [{"$and": [{"count": {"$gte": 90}}], {"count": {"$lt": 10}}]}]})");
}