#include <actor-zeta.hpp>
#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/ql/aggregate.hpp>
#include <components/ql/aggregate/limit.hpp>
#include <components/ql/statements/create_collection.hpp>
#include <components/ql/statements/create_database.hpp>
#include <components/ql/statements/delete_many.hpp>
#include <components/ql/statements/delete_one.hpp>
#include <components/ql/statements/drop_collection.hpp>
#include <components/ql/statements/drop_database.hpp>
#include <components/ql/statements/insert_many.hpp>
#include <components/ql/statements/insert_one.hpp>
#include <components/ql/statements/update_many.hpp>
#include <components/ql/statements/update_one.hpp>
#include <components/tests/generaty.hpp>
#include <components/translator/ql_translator.hpp>

using namespace components::translator;
using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

collection_full_name_t get_name() { return {database_name, collection_name}; }

TEST_CASE("logical_plan::create_database") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    components::ql::create_database_t ql{database_name};
    auto node = ql_translator(resource, &ql);
    REQUIRE(node->to_string() == R"_($create_database: database)_");
}

TEST_CASE("logical_plan::drop_database") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    components::ql::drop_database_t ql{database_name};
    auto node = ql_translator(resource, &ql);
    REQUIRE(node->to_string() == R"_($drop_database: database)_");
}

TEST_CASE("logical_plan::create_collection") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    components::ql::create_collection_t ql{database_name, collection_name};
    auto node = ql_translator(resource, &ql);
    REQUIRE(node->to_string() == R"_($create_collection: database.collection)_");
}

TEST_CASE("logical_plan::drop_collection") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    components::ql::drop_collection_t ql{database_name, collection_name};
    auto node = ql_translator(resource, &ql);
    REQUIRE(node->to_string() == R"_($drop_collection: database.collection)_");
}

TEST_CASE("logical_plan::match") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto match = components::ql::aggregate::make_match(
        make_compare_expression(resource, compare_type::eq, key("key"), core::parameter_id_t(1)));
    auto node_match = make_node_match(resource, get_name(), match);
    REQUIRE(node_match->to_string() == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("logical_plan::group") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    {
        components::ql::aggregate::group_t group;
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
        auto node_group = make_node_group(resource, get_name(), group);
        REQUIRE(
            node_group->to_string() ==
            R"_($group: {_id: "$date", total: {$sum: {$multiply: ["$price", "$quantity"]}}, avg_quantity: {$avg: "$quantity"}})_");
    }
    {
        components::ql::aggregate::group_t group;
        auto scalar_expr = make_scalar_expression(resource, scalar_type::get_field, key("_id"));
        scalar_expr->append_param(key("date"));
        append_expr(group, std::move(scalar_expr));
        scalar_expr = make_scalar_expression(resource, scalar_type::multiply, key("count_4"));
        scalar_expr->append_param(core::parameter_id_t(1));
        scalar_expr->append_param(key("count"));
        append_expr(group, std::move(scalar_expr));
        auto node_group = make_node_group(resource, get_name(), group);
        REQUIRE(node_group->to_string() == R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}})_");
    }
}

TEST_CASE("logical_plan::sort") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    {
        components::ql::aggregate::sort_t sort;
        components::ql::aggregate::append_sort(sort, key("key"), sort_order::asc);
        auto node_sort = make_node_sort(resource, get_name(), sort);
        REQUIRE(node_sort->to_string() == R"_($sort: {key: 1})_");
    }
    {
        components::ql::aggregate::sort_t sort;
        components::ql::aggregate::append_sort(sort, key("key1"), sort_order::asc);
        components::ql::aggregate::append_sort(sort, key("key2"), sort_order::desc);
        auto node_sort = make_node_sort(resource, get_name(), sort);
        REQUIRE(node_sort->to_string() == R"_($sort: {key1: 1, key2: -1})_");
    }
}

TEST_CASE("logical_plan::aggregate") {
    using components::ql::aggregate::operator_type;

    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    components::ql::aggregate_statement aggregate(database_name, collection_name);

    aggregate.append(operator_type::match,
                     components::ql::aggregate::make_match(
                         make_compare_expression(resource, compare_type::eq, key("key"), core::parameter_id_t(1))));

    components::ql::aggregate::group_t group;
    auto scalar_expr = make_scalar_expression(resource, scalar_type::get_field, key("_id"));
    scalar_expr->append_param(key("date"));
    append_expr(group, std::move(scalar_expr));
    scalar_expr = make_scalar_expression(resource, scalar_type::multiply, key("count_4"));
    scalar_expr->append_param(core::parameter_id_t(1));
    scalar_expr->append_param(key("count"));
    append_expr(group, std::move(scalar_expr));
    aggregate.append(operator_type::group, std::move(group));

    components::ql::aggregate::sort_t sort;
    append_sort(sort, key("name"), sort_order::asc);
    append_sort(sort, key("count"), sort_order::desc);
    aggregate.append(operator_type::sort, std::move(sort));

    auto node_aggregate = ql_translator(resource, &aggregate);

    REQUIRE(node_aggregate->to_string() == R"_($aggregate: {)_"
                                           R"_($match: {"key": {$eq: #1}}, )_"
                                           R"_($group: {_id: "$date", count_4: {$multiply: [#1, "$count"]}}, )_"
                                           R"_($sort: {name: 1, count: -1})_"
                                           R"_(})_");
}

TEST_CASE("logical_plan::insert") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    {
        std::pmr::vector<components::document::document_ptr> documents = {};
        components::ql::insert_many_t insert{database_name, collection_name, documents};
        auto node = ql_translator(resource, &insert);
        REQUIRE(node->to_string() == R"_($insert: {$documents: 0})_");
    }
    {
        std::pmr::vector<components::document::document_ptr> documents = {gen_doc(1)};
        components::ql::insert_many_t insert{database_name, collection_name, documents};
        auto node = ql_translator(resource, &insert);
        REQUIRE(node->to_string() == R"_($insert: {$documents: 1})_");
    }
    {
        std::pmr::vector<components::document::document_ptr> documents = {gen_doc(1),
                                                                          gen_doc(2),
                                                                          gen_doc(3),
                                                                          gen_doc(4),
                                                                          gen_doc(5)};
        components::ql::insert_many_t insert{database_name, collection_name, documents};
        auto node = ql_translator(resource, &insert);
        REQUIRE(node->to_string() == R"_($insert: {$documents: 5})_");
    }
    {
        std::pmr::vector<components::document::document_ptr> documents = {};
        components::ql::insert_one_t insert{database_name, collection_name, gen_doc(1)};
        auto node = ql_translator(resource, &insert);
        REQUIRE(node->to_string() == R"_($insert: {$documents: 1})_");
    }
}

TEST_CASE("logical_plan::limit") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    {
        auto limit = components::ql::limit_t::limit_one();
        auto node_limit = components::logical_plan::make_node_limit(resource, get_name(), limit);
        REQUIRE(node_limit->to_string() == R"_($limit: 1)_");
    }
    {
        auto limit = components::ql::limit_t::unlimit();
        auto node_limit = components::logical_plan::make_node_limit(resource, get_name(), limit);
        REQUIRE(node_limit->to_string() == R"_($limit: -1)_");
    }
    {
        auto limit = components::ql::limit_t(5);
        auto node_limit = components::logical_plan::make_node_limit(resource, get_name(), limit);
        REQUIRE(node_limit->to_string() == R"_($limit: 5)_");
    }
}

TEST_CASE("logical_plan::delete") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto match = components::ql::aggregate::make_match(
        make_compare_expression(resource, compare_type::eq, key("key"), core::parameter_id_t(1)));
    components::ql::storage_parameters parameters{};
    {
        auto ql_delete = components::ql::delete_many_t(database_name, collection_name, match, parameters);
        auto node_delete = ql_translator(resource, &ql_delete);
        REQUIRE(node_delete->to_string() == R"_($delete: {$match: {"key": {$eq: #1}}, $limit: -1})_");
    }
    {
        auto ql_delete = components::ql::delete_one_t(database_name, collection_name, match, parameters);
        auto node_delete = ql_translator(resource, &ql_delete);
        REQUIRE(node_delete->to_string() == R"_($delete: {$match: {"key": {$eq: #1}}, $limit: 1})_");
    }
}

TEST_CASE("logical_plan::update") {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto match = components::ql::aggregate::make_match(
        make_compare_expression(resource, compare_type::eq, key("key"), core::parameter_id_t(1)));
    auto update = document_from_json(R"_({"$set": {"count": 100}})_");
    components::ql::storage_parameters parameters{};
    {
        auto ql_update = components::ql::update_many_t(database_name, collection_name, match, parameters, update, true);
        auto node_update = ql_translator(resource, &ql_update);
        REQUIRE(node_update->to_string() ==
                R"_($update: {{"$set":{"count":100}}, $upsert: 1, $match: {"key": {$eq: #1}}, $limit: -1})_");
    }
    {
        auto ql_update = components::ql::update_one_t(database_name, collection_name, match, parameters, update, false);
        auto node_update = ql_translator(resource, &ql_update);
        REQUIRE(node_update->to_string() ==
                R"_($update: {{"$set":{"count":100}}, $upsert: 0, $match: {"key": {$eq: #1}}, $limit: 1})_");
    }
}
