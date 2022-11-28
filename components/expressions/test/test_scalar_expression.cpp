#include <catch2/catch.hpp>
#include <components/expressions/scalar_expression.hpp>

//using namespace components::expressions;
//using key = components::expressions::key_t;

//TEST_CASE("expression::scalar::equals") {
//    auto expr1 = make_compare_expression(compare_type::eq, key("name"), core::parameter_id_t(1));
//    auto expr2 = make_compare_expression(compare_type::eq, key("name"), core::parameter_id_t(1));
//    auto expr3 = make_compare_expression(compare_type::ne, key("name"), core::parameter_id_t(1));
//    auto expr4 = make_compare_expression(compare_type::eq, key("count"), core::parameter_id_t(1));
//    auto expr5 = make_compare_expression(compare_type::eq, key("name"), core::parameter_id_t(2));
//    auto expr_union1 = make_compare_union_expression(compare_type::union_and);
//    expr_union1->append_child(expr1);
//    expr_union1->append_child(expr3);
//    auto expr_union2 = make_compare_union_expression(compare_type::union_and);
//    expr_union2->append_child(expr1);
//    expr_union2->append_child(expr3);
//    auto expr_union3 = make_compare_union_expression(compare_type::union_and);
//    expr_union3->append_child(expr1);
//    expr_union3->append_child(expr4);
//    auto expr_union4 = make_compare_union_expression(compare_type::union_or);
//    expr_union4->append_child(expr1);
//    expr_union4->append_child(expr3);
//    REQUIRE(expression_equal()(expr1, expr2));
//    REQUIRE_FALSE(expression_equal()(expr1, expr3));
//    REQUIRE_FALSE(expression_equal()(expr1, expr4));
//    REQUIRE_FALSE(expression_equal()(expr1, expr5));
//    REQUIRE(expression_equal()(expr_union1, expr_union2));
//    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union3));
//    REQUIRE_FALSE(expression_equal()(expr_union1, expr_union4));
//}
//
//TEST_CASE("expression::scalar::to_string") {
//    auto expr = make_compare_expression(compare_type::eq, key("count"), core::parameter_id_t(1));
//    REQUIRE(expr->to_string() == R"({"count": {$eq: #1}})");
//
//    expr = make_compare_union_expression(compare_type::union_and);
//    expr->append_child(make_compare_expression(compare_type::eq, key("key1"), core::parameter_id_t(1)));
//    expr->append_child(make_compare_expression(compare_type::lt, key("key2"), core::parameter_id_t(2)));
//    REQUIRE(expr->to_string() == R"({$and: [{"key1": {$eq: #1}}, {"key2": {$lt: #2}}]})");
//}

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