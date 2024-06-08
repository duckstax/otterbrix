#include "test_operator_generaty.hpp"
#include <catch2/catch.hpp>
#include <components/document/support/varint.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>

using namespace components::expressions;
using namespace services::collection::operators;
using namespace services::collection::operators::aggregate;
using key = components::expressions::key_t;
using components::ql::add_parameter;

TEST_CASE("operator::aggregate::count") {
    auto collection = init_collection();

    SECTION("count::all") {
        operator_count_t count(d(collection));
        auto cond = make_compare_expression(d(collection)->resource(), compare_type::all_true);
        count.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                              predicates::create_predicate(d(collection), cond),
                                                              components::ql::limit_t::unlimit())));
        count.on_execute(nullptr);
        REQUIRE(count.value()->as_unsigned() == 100);
    }

    SECTION("count::match") {
        auto cond = make_compare_expression(d(collection)->resource(),
                                            compare_type::lte,
                                            key("count"),
                                            core::parameter_id_t(1));
        operator_count_t count(d(collection));
        count.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                              predicates::create_predicate(d(collection), cond),
                                                              components::ql::limit_t::unlimit())));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 10);
        components::pipeline::context_t pipeline_context(std::move(parameters));
        count.on_execute(&pipeline_context);
        REQUIRE(count.value()->as_unsigned() == 10);
    }
}

TEST_CASE("operator::aggregate::min") {
    auto collection = init_collection();

    SECTION("min::all") {
        auto cond = make_compare_expression(d(collection)->resource(), compare_type::all_true);
        operator_min_t min_(d(collection), key("count"));
        min_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        min_.on_execute(nullptr);
        REQUIRE(min_.value()->as_unsigned() == 1);
    }

    SECTION("min::match") {
        auto cond =
            make_compare_expression(d(collection)->resource(), compare_type::gt, key("count"), core::parameter_id_t(1));
        operator_min_t min_(d(collection), key("count"));
        min_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 80);
        components::pipeline::context_t pipeline_context(std::move(parameters));
        min_.on_execute(&pipeline_context);
        REQUIRE(min_.value()->as_unsigned() == 81);
    }
}

TEST_CASE("operator::aggregate::max") {
    auto collection = init_collection();

    SECTION("max::all") {
        auto cond = make_compare_expression(d(collection)->resource(), compare_type::all_true);
        operator_max_t max_(d(collection), key("count"));
        max_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        max_.on_execute(nullptr);
        REQUIRE(max_.value()->as_unsigned() == 100);
    }

    SECTION("max::match") {
        auto cond =
            make_compare_expression(d(collection)->resource(), compare_type::lt, key("count"), core::parameter_id_t(1));
        operator_max_t max_(d(collection), key("count"));
        max_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 20);
        components::pipeline::context_t pipeline_context(std::move(parameters));
        max_.on_execute(&pipeline_context);
        REQUIRE(max_.value()->as_unsigned() == 19);
    }
}

TEST_CASE("operator::aggregate::sum") {
    auto collection = init_collection();

    SECTION("sum::all") {
        auto cond = make_compare_expression(d(collection)->resource(), compare_type::all_true);
        operator_sum_t sum_(d(collection), key("count"));
        sum_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        sum_.on_execute(nullptr);
        REQUIRE(sum_.value()->as_unsigned() == 5050);
    }

    SECTION("sum::match") {
        auto cond =
            make_compare_expression(d(collection)->resource(), compare_type::lt, key("count"), core::parameter_id_t(1));
        operator_sum_t sum_(d(collection), key("count"));
        sum_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 10);
        components::pipeline::context_t pipeline_context(std::move(parameters));
        sum_.on_execute(&pipeline_context);
        REQUIRE(sum_.value()->as_unsigned() == 45);
    }
}

TEST_CASE("operator::aggregate::avg") {
    auto collection = init_collection();

    SECTION("avg::all") {
        auto cond = make_compare_expression(d(collection)->resource(), compare_type::all_true);
        operator_avg_t avg_(d(collection), key("count"));
        avg_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        avg_.on_execute(nullptr);
        REQUIRE(::document::is_equals(avg_.value()->as_double(), 50.5));
    }

    SECTION("avg::match") {
        auto cond =
            make_compare_expression(d(collection)->resource(), compare_type::lt, key("count"), core::parameter_id_t(1));
        operator_avg_t avg_(d(collection), key("count"));
        avg_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                             predicates::create_predicate(d(collection), cond),
                                                             components::ql::limit_t::unlimit())));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 10);
        components::pipeline::context_t pipeline_context(std::move(parameters));
        avg_.on_execute(&pipeline_context);
        REQUIRE(::document::is_equals(avg_.value()->as_double(), 5.0));
    }
}
