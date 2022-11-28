#include <catch2/catch.hpp>
#include <components/document/support/varint.hpp>
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/aggregate/operator_count.hpp>
#include <services/collection/operators/aggregate/operator_min.hpp>
#include <services/collection/operators/aggregate/operator_max.hpp>
#include <services/collection/operators/aggregate/operator_sum.hpp>
#include <services/collection/operators/aggregate/operator_avg.hpp>
#include "test_operator_generaty.hpp"

using namespace components;
using namespace services::collection::operators;
using namespace services::collection::operators::aggregate;

TEST_CASE("operator::aggregate::count") {
    auto collection = init_collection();

    SECTION("count::all") {
        auto cond = parse_find_condition("{}");
        operator_count_t count(d(collection)->view());
        count.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                       predicates::create_predicate(d(collection)->view(), cond),
                                                       predicates::limit_t::unlimit()));
        count.on_execute(nullptr);
        REQUIRE(count.value()->as_unsigned() == 100);
    }

    SECTION("count::match") {
        auto cond = parse_find_condition(R"({"count": {"$lte": 10}})");
        operator_count_t count(d(collection)->view());
        count.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                       predicates::create_predicate(d(collection)->view(), cond),
                                                       predicates::limit_t::unlimit()));
        count.on_execute(nullptr);
        REQUIRE(count.value()->as_unsigned() == 10);
    }
}

TEST_CASE("operator::aggregate::min") {
    auto collection = init_collection();

    SECTION("min::all") {
        auto cond = parse_find_condition("{}");
        operator_min_t min_(d(collection)->view(), ql::key_t("count"));
        min_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        min_.on_execute(nullptr);
        REQUIRE(min_.value()->as_unsigned() == 1);
    }

    SECTION("min::match") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 80}})");
        operator_min_t min_(d(collection)->view(), ql::key_t("count"));
        min_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        min_.on_execute(nullptr);
        REQUIRE(min_.value()->as_unsigned() == 81);
    }
}

TEST_CASE("operator::aggregate::max") {
    auto collection = init_collection();

    SECTION("max::all") {
        auto cond = parse_find_condition("{}");
        operator_max_t max_(d(collection)->view(), ql::key_t("count"));
        max_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        max_.on_execute(nullptr);
        REQUIRE(max_.value()->as_unsigned() == 100);
    }

    SECTION("max::match") {
        auto cond = parse_find_condition(R"({"count": {"$lt": 20}})");
        operator_max_t max_(d(collection)->view(), ql::key_t("count"));
        max_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        max_.on_execute(nullptr);
        REQUIRE(max_.value()->as_unsigned() == 19);
    }
}

TEST_CASE("operator::aggregate::sum") {
    auto collection = init_collection();

    SECTION("sum::all") {
        auto cond = parse_find_condition("{}");
        operator_sum_t sum_(d(collection)->view(), ql::key_t("count"));
        sum_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        sum_.on_execute(nullptr);
        REQUIRE(sum_.value()->as_unsigned() == 5050);
    }

    SECTION("sum::match") {
        auto cond = parse_find_condition(R"({"count": {"$lt": 10}})");
        operator_sum_t sum_(d(collection)->view(), ql::key_t("count"));
        sum_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        sum_.on_execute(nullptr);
        REQUIRE(sum_.value()->as_unsigned() == 45);
    }
}

TEST_CASE("operator::aggregate::avg") {
    auto collection = init_collection();

    SECTION("avg::all") {
        auto cond = parse_find_condition("{}");
        operator_avg_t avg_(d(collection)->view(), ql::key_t("count"));
        avg_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        avg_.on_execute(nullptr);
        REQUIRE(::document::is_equals(avg_.value()->as_double(), 50.5));
    }

    SECTION("avg::match") {
        auto cond = parse_find_condition(R"({"count": {"$lt": 10}})");
        operator_avg_t avg_(d(collection)->view(), ql::key_t("count"));
        avg_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                      predicates::create_predicate(d(collection)->view(), cond),
                                                      predicates::limit_t::unlimit()));
        avg_.on_execute(nullptr);
        REQUIRE(::document::is_equals(avg_.value()->as_double(), 5.0));
    }
}
