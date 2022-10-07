#include <catch2/catch.hpp>
#include <services/collection/operators/operator_group.hpp>
#include <services/collection/operators/scan/transfer_scan.hpp>
#include <services/collection/operators/get/simple_value.hpp>
#include "test_operator_generaty.hpp"

using namespace services::collection::operators;

TEST_CASE("operator::group::base") {
    auto collection = init_collection();

    SECTION("base::all::no_valid") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), predicates::limit_t::unlimit()));
        group.add_key("id_", get::simple_value_t::create("id_"));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 0);
    }

    SECTION("base::all::id") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), predicates::limit_t::unlimit()));
        group.add_key("_id", get::simple_value_t::create("_id"));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 100);
    }

    SECTION("base::all::countBool") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), predicates::limit_t::unlimit()));
        group.add_key("countBool", get::simple_value_t::create("countBool"));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 2);
    }

    SECTION("base::all::dict") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), predicates::limit_t::unlimit()));
        group.add_key("even", get::simple_value_t::create("countDict.even"));
        group.add_key("three", get::simple_value_t::create("countDict.three"));
        group.add_key("five", get::simple_value_t::create("countDict.five"));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 8);
    }
}