#include <catch2/catch.hpp>
#include <services/collection/operators/full_scan.hpp>
#include <services/collection/operators/aggregate/operator_count.hpp>
#include "test_operator_generaty.hpp"

using namespace services::collection::operators;
using namespace services::collection::operators::aggregate;

TEST_CASE("operator::aggregate::count") {
    auto collection = init_collection();

//    SECTION("count::all") {
//        auto cond = parse_find_condition("{}");
//        operator_count_t count(d(collection)->view());
//        count.set_children(std::make_unique<full_scan>(d(collection)->view(),
//                                                       predicates::create_predicate(d(collection)->view(), cond),
//                                                       predicates::limit_t::unlimit()));
//        count.on_execute(nullptr);
//        REQUIRE(document_view_t(count.output()->documents().at(0)).get_ulong(count.key()) == 100);
//    }

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