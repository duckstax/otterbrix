#include <catch2/catch.hpp>
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/merge/operator_and.hpp>
#include <services/collection/operators/merge/operator_or.hpp>
#include <services/collection/operators/merge/operator_not.hpp>
#include "test_operator_generaty.hpp"

using namespace services::collection::operators;
using namespace services::collection::operators::merge;

TEST_CASE("operator_merge::and") {
    auto collection = init_collection();
    auto cond1 = parse_find_condition(R"({"count": {"$gt": 50}})");
    auto cond2 = parse_find_condition(R"({"count": {"$lte": 60}})");
    operator_and_t op_and(d(collection)->view(), predicates::limit_t::unlimit());
    op_and.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond1),
                                                    predicates::limit_t::unlimit()),
                        std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond2),
                                                    predicates::limit_t::unlimit()));
    op_and.on_execute(nullptr);
    REQUIRE(op_and.output()->size() == 10);
}

TEST_CASE("operator_merge::or") {
    auto collection = init_collection();
    auto cond1 = parse_find_condition(R"({"count": {"$lte": 10}})");
    auto cond2 = parse_find_condition(R"({"count": {"$gt": 90}})");
    operator_or_t op_or(d(collection)->view(), predicates::limit_t::unlimit());
    op_or.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                   predicates::create_predicate(d(collection)->view(), cond1),
                                                   predicates::limit_t::unlimit()),
                        std::make_unique<full_scan>(d(collection)->view(),
                                                   predicates::create_predicate(d(collection)->view(), cond2),
                                                   predicates::limit_t::unlimit()));
    op_or.on_execute(nullptr);
    REQUIRE(op_or.output()->size() == 20);
}

TEST_CASE("operator_merge::not") {
    auto collection = init_collection();
    auto cond = parse_find_condition(R"({"count": {"$gt": 10}})");
    operator_not_t op_not(d(collection)->view(), predicates::limit_t::unlimit());
    op_not.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond),
                                                    predicates::limit_t::unlimit()));
    op_not.on_execute(nullptr);
    REQUIRE(op_not.output()->size() == 10);
}

TEST_CASE("operator_merge::complex") {
    auto collection = init_collection();
    auto cond = parse_find_condition(R"(
{
  "$and": [
    {"$or": [{"count": {"$lte": 10}}, {"count": {"$gt": 90}}]},
    {"$and": [{"count": {"$gt": 5}}, {"count": {"$lte": 95}}]}
  ]
}
)");
    auto op = create_operator_merge(d(collection)->view(), cond, predicates::limit_t::unlimit());
    const auto &cond_or = cond->condition_->sub_conditions_.at(0);
    const auto &cond_and = cond->condition_->sub_conditions_.at(1);
    auto op_or = create_operator_merge(d(collection)->view(), cond_or, predicates::limit_t::unlimit());
    op_or->set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond_or->sub_conditions_.at(0)),
                                                    predicates::limit_t::unlimit()),
                        std::make_unique<full_scan>(d(collection)->view(),
                                                    predicates::create_predicate(d(collection)->view(), cond_or->sub_conditions_.at(1)),
                                                    predicates::limit_t::unlimit()));
    auto op_and = create_operator_merge(d(collection)->view(), cond_and, predicates::limit_t::unlimit());
    op_and->set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                     predicates::create_predicate(d(collection)->view(), cond_and->sub_conditions_.at(0)),
                                                     predicates::limit_t::unlimit()),
                         std::make_unique<full_scan>(d(collection)->view(),
                                                     predicates::create_predicate(d(collection)->view(), cond_and->sub_conditions_.at(1)),
                                                     predicates::limit_t::unlimit()));
    op->set_children(std::move(op_or), std::move(op_and));
    op->on_execute(nullptr);
    REQUIRE(op->output()->size() == 10);
}
