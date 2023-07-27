#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/merge/operator_and.hpp>
#include <services/collection/operators/merge/operator_or.hpp>
#include <services/collection/operators/merge/operator_not.hpp>
#include "test_operator_generaty.hpp"

using namespace components::expressions;
using namespace services::collection::operators;
using namespace services::collection::operators::merge;
using key = components::expressions::key_t;
using components::ql::add_parameter;

TEST_CASE("operator_merge::and") {
    auto collection = init_collection();
    auto cond1 = make_compare_expression(d(collection)->view()->resource(),
                                         compare_type::gt,
                                         key("count"),
                                         core::parameter_id_t(1));
    auto cond2 = make_compare_expression(d(collection)->view()->resource(),
                                         compare_type::lte,
                                         key("count"),
                                         core::parameter_id_t(2));
    operator_and_t op_and(d(collection)->view(), predicates::limit_t::unlimit());
    op_and.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond1),
                                                    predicates::limit_t::unlimit()),
                        std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond2),
                                                    predicates::limit_t::unlimit()));
    components::ql::storage_parameters parameters;
    add_parameter(parameters, core::parameter_id_t(1), 50);
    add_parameter(parameters, core::parameter_id_t(2), 60);
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op_and.on_execute(&pipeline_context);
    REQUIRE(op_and.output()->size() == 10);
}

TEST_CASE("operator_merge::or") {
    auto collection = init_collection();
    auto cond1 = make_compare_expression(d(collection)->view()->resource(),
                                         compare_type::lte,
                                         key("count"),
                                         core::parameter_id_t(1));
    auto cond2 = make_compare_expression(d(collection)->view()->resource(),
                                         compare_type::gt,
                                         key("count"),
                                         core::parameter_id_t(2));
    operator_or_t op_or(d(collection)->view(), predicates::limit_t::unlimit());
    op_or.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                   predicates::create_predicate(d(collection)->view(), cond1),
                                                   predicates::limit_t::unlimit()),
                        std::make_unique<full_scan>(d(collection)->view(),
                                                   predicates::create_predicate(d(collection)->view(), cond2),
                                                   predicates::limit_t::unlimit()));
    components::ql::storage_parameters parameters;
    add_parameter(parameters, core::parameter_id_t(1), 10);
    add_parameter(parameters, core::parameter_id_t(2), 90);
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op_or.on_execute(&pipeline_context);
    REQUIRE(op_or.output()->size() == 20);
}

TEST_CASE("operator_merge::not") {
    auto collection = init_collection();
    auto cond = make_compare_expression(d(collection)->view()->resource(),
                                        compare_type::gt,
                                        key("count"),
                                        core::parameter_id_t(1));
    operator_not_t op_not(d(collection)->view(), predicates::limit_t::unlimit());
    op_not.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond),
                                                    predicates::limit_t::unlimit()));
    components::ql::storage_parameters parameters;
    add_parameter(parameters, core::parameter_id_t(1), 10);
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op_not.on_execute(&pipeline_context);
    REQUIRE(op_not.output()->size() == 10);
}

TEST_CASE("operator_merge::complex") {
    auto collection = init_collection();
//  "$and": [
//    {"$or": [{"count": {"$lte": 10}}, {"count": {"$gt": 90}}]},
//    {"$and": [{"count": {"$gt": 5}}, {"count": {"$lte": 95}}]}
//  ]

    auto cond_or1 = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::lte,
                                            key("count"),
                                            core::parameter_id_t(1));
    auto cond_or2 = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(2));
    auto cond_and1 = make_compare_expression(d(collection)->view()->resource(),
                                             compare_type::gt,
                                             key("count"),
                                             core::parameter_id_t(3));
    auto cond_and2 = make_compare_expression(d(collection)->view()->resource(),
                                             compare_type::lte,
                                             key("count"),
                                             core::parameter_id_t(4));

    auto op = create_operator_merge(d(collection)->view(), compare_type::union_and, predicates::limit_t::unlimit());
    auto op_or = create_operator_merge(d(collection)->view(), compare_type::union_or, predicates::limit_t::unlimit());
    auto op_and = create_operator_merge(d(collection)->view(), compare_type::union_and, predicates::limit_t::unlimit());
    op_or->set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond_or1),
                                                    predicates::limit_t::unlimit()),
                        std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond_or2),
                                                    predicates::limit_t::unlimit()));
    op_and->set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                     predicates::create_predicate(d(collection)->view(), cond_and1),
                                                     predicates::limit_t::unlimit()),
                         std::make_unique<full_scan>(d(collection)->view(),
                                                     predicates::create_predicate(d(collection)->view(), cond_and2),
                                                     predicates::limit_t::unlimit()));
    op->set_children(std::move(op_or), std::move(op_and));
    components::ql::storage_parameters parameters;
    add_parameter(parameters, core::parameter_id_t(1), 10);
    add_parameter(parameters, core::parameter_id_t(2), 90);
    add_parameter(parameters, core::parameter_id_t(3), 5);
    add_parameter(parameters, core::parameter_id_t(4), 95);
    components::pipeline::context_t pipeline_context(std::move(parameters));
    op->on_execute(&pipeline_context);
    REQUIRE(op->output()->size() == 10);
}
