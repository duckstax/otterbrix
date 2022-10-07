#include <catch2/catch.hpp>
#include <services/collection/operators/operator_group.hpp>
#include <services/collection/operators/operator_sort.hpp>
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

TEST_CASE("operator::group::sort") {
    auto collection = init_collection();

    SECTION("sort::all") {
        auto group = std::make_unique<operator_group_t>(d(collection)->view());
        group->set_children(std::make_unique<transfer_scan>(d(collection)->view(), predicates::limit_t::unlimit()));
        group->add_key("even", get::simple_value_t::create("countDict.even"));
        group->add_key("three", get::simple_value_t::create("countDict.three"));
        group->add_key("five", get::simple_value_t::create("countDict.five"));
        auto sort = std::make_unique<operator_sort_t>(d(collection)->view());
        sort->set_children(std::move(group));
        sort->add({"even", "three", "five"});
        sort->on_execute(nullptr);
        REQUIRE(sort->output()->size() == 8);

        auto check = [](const document_ptr &doc, bool is1, bool is2, bool is3) {
            REQUIRE(document_view_t(doc).get_bool("even") == is1);
            REQUIRE(document_view_t(doc).get_bool("three") == is2);
            REQUIRE(document_view_t(doc).get_bool("five") == is3);
        };
        check(sort->output()->documents().at(0), false, false, false);
        check(sort->output()->documents().at(1), false, false, true);
        check(sort->output()->documents().at(2), false, true, false);
        check(sort->output()->documents().at(3), false, true, true);
        check(sort->output()->documents().at(4), true, false, false);
        check(sort->output()->documents().at(5), true, false, true);
        check(sort->output()->documents().at(6), true, true, false);
        check(sort->output()->documents().at(7), true, true, true);
    }
}
