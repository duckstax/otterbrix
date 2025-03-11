#include "test_operator_generaty.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/collection/operators/merge/operator_and.hpp>
#include <components/physical_plan/collection/operators/merge/operator_not.hpp>
#include <components/physical_plan/collection/operators/merge/operator_or.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>

using namespace components::expressions;
using namespace services::collection::operators;
using namespace services::collection::operators::merge;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("operator_merge::and") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    auto collection = init_collection(&resource);
    auto cond1 = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
    auto cond2 = make_compare_expression(&resource, compare_type::lte, key("count"), core::parameter_id_t(2));
    operator_and_t op_and(d(collection), components::logical_plan::limit_t::unlimit());
    op_and.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                           predicates::create_predicate(d(collection), cond1),
                                                           components::logical_plan::limit_t::unlimit())),
                        boost::intrusive_ptr(new full_scan(d(collection),
                                                           predicates::create_predicate(d(collection), cond2),
                                                           components::logical_plan::limit_t::unlimit())));
    components::logical_plan::storage_parameters parameters(&resource);
    add_parameter(parameters, core::parameter_id_t(1), new_value(50));
    add_parameter(parameters, core::parameter_id_t(2), new_value(60));
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op_and.on_execute(&pipeline_context);
    REQUIRE(op_and.output()->size() == 10);
}

TEST_CASE("operator_merge::or") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    auto collection = init_collection(&resource);
    auto cond1 = make_compare_expression(&resource, compare_type::lte, key("count"), core::parameter_id_t(1));
    auto cond2 = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(2));
    operator_or_t op_or(d(collection), components::logical_plan::limit_t::unlimit());
    op_or.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                          predicates::create_predicate(d(collection), cond1),
                                                          components::logical_plan::limit_t::unlimit())),
                       boost::intrusive_ptr(new full_scan(d(collection),
                                                          predicates::create_predicate(d(collection), cond2),
                                                          components::logical_plan::limit_t::unlimit())));
    components::logical_plan::storage_parameters parameters(&resource);
    add_parameter(parameters, core::parameter_id_t(1), new_value(10));
    add_parameter(parameters, core::parameter_id_t(2), new_value(90));
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op_or.on_execute(&pipeline_context);
    REQUIRE(op_or.output()->size() == 20);
}

TEST_CASE("operator_merge::not") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    auto collection = init_collection(&resource);
    auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
    operator_not_t op_not(d(collection), components::logical_plan::limit_t::unlimit());
    op_not.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                           predicates::create_predicate(d(collection), cond),
                                                           components::logical_plan::limit_t::unlimit())));
    components::logical_plan::storage_parameters parameters(&resource);
    add_parameter(parameters, core::parameter_id_t(1), new_value(10));
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op_not.on_execute(&pipeline_context);
    REQUIRE(op_not.output()->size() == 10);
}

TEST_CASE("operator_merge::complex") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    auto collection = init_collection(&resource);
    //  "$and": [
    //    {"$or": [{"count": {"$lte": 10}}, {"count": {"$gt": 90}}]},
    //    {"$and": [{"count": {"$gt": 5}}, {"count": {"$lte": 95}}]}
    //  ]

    auto cond_or1 = make_compare_expression(&resource, compare_type::lte, key("count"), core::parameter_id_t(1));
    auto cond_or2 = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(2));
    auto cond_and1 = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(3));
    auto cond_and2 = make_compare_expression(&resource, compare_type::lte, key("count"), core::parameter_id_t(4));

    auto op =
        create_operator_merge(d(collection), compare_type::union_and, components::logical_plan::limit_t::unlimit());
    auto op_or =
        create_operator_merge(d(collection), compare_type::union_or, components::logical_plan::limit_t::unlimit());
    auto op_and =
        create_operator_merge(d(collection), compare_type::union_and, components::logical_plan::limit_t::unlimit());
    op_or->set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                           predicates::create_predicate(d(collection), cond_or1),
                                                           components::logical_plan::limit_t::unlimit())),
                        boost::intrusive_ptr(new full_scan(d(collection),
                                                           predicates::create_predicate(d(collection), cond_or2),
                                                           components::logical_plan::limit_t::unlimit())));
    op_and->set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                            predicates::create_predicate(d(collection), cond_and1),
                                                            components::logical_plan::limit_t::unlimit())),
                         boost::intrusive_ptr(new full_scan(d(collection),
                                                            predicates::create_predicate(d(collection), cond_and2),
                                                            components::logical_plan::limit_t::unlimit())));
    op->set_children(std::move(op_or), std::move(op_and));
    components::logical_plan::storage_parameters parameters(&resource);
    add_parameter(parameters, core::parameter_id_t(1), new_value(10));
    add_parameter(parameters, core::parameter_id_t(2), new_value(90));
    add_parameter(parameters, core::parameter_id_t(3), new_value(5));
    add_parameter(parameters, core::parameter_id_t(4), new_value(95));
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op->on_execute(&pipeline_context);
    REQUIRE(op->output()->size() == 10);
}
