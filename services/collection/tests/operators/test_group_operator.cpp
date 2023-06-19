#include <catch2/catch.hpp>
#include <services/collection/operators/operator_group.hpp>
#include <services/collection/operators/operator_sort.hpp>
#include <services/collection/operators/scan/transfer_scan.hpp>
#include <services/collection/operators/get/simple_value.hpp>
#include <services/collection/operators/aggregate/operator_count.hpp>
#include <services/collection/operators/aggregate/operator_sum.hpp>
#include <services/collection/operators/aggregate/operator_avg.hpp>
#include "test_operator_generaty.hpp"

using namespace components;
using namespace services::collection::operators;
using key = components::expressions::key_t;

TEST_CASE("operator::group::base") {
    auto collection = init_collection();

    SECTION("base::all::no_valid") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), components::ql::limit_t::unlimit()));
        group.add_key("id_", get::simple_value_t::create(key("id_")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 0);
    }

    SECTION("base::all::id") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), components::ql::limit_t::unlimit()));
        group.add_key("_id", get::simple_value_t::create(key("_id")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 100);
    }

    SECTION("base::all::countBool") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), components::ql::limit_t::unlimit()));
        group.add_key("countBool", get::simple_value_t::create(key("countBool")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 2);
    }

    SECTION("base::all::dict") {
        operator_group_t group(d(collection)->view());
        group.set_children(std::make_unique<transfer_scan>(d(collection)->view(), components::ql::limit_t::unlimit()));
        group.add_key("even", get::simple_value_t::create(key("countDict.even")));
        group.add_key("three", get::simple_value_t::create(key("countDict.three")));
        group.add_key("five", get::simple_value_t::create(key("countDict.five")));
        group.on_execute(nullptr);
        REQUIRE(group.output()->size() == 8);
    }
}

TEST_CASE("operator::group::sort") {
    auto collection = init_collection();

    SECTION("sort::all") {
        auto group = std::make_unique<operator_group_t>(d(collection)->view());
        group->set_children(std::make_unique<transfer_scan>(d(collection)->view(), components::ql::limit_t::unlimit()));
        group->add_key("even", get::simple_value_t::create(key("countDict.even")));
        group->add_key("three", get::simple_value_t::create(key("countDict.three")));
        group->add_key("five", get::simple_value_t::create(key("countDict.five")));
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

TEST_CASE("operator::group::all") {
    auto collection = init_collection();

    SECTION("sort::all") {
        auto group = std::make_unique<operator_group_t>(d(collection)->view());
        group->set_children(std::make_unique<transfer_scan>(d(collection)->view(), components::ql::limit_t::unlimit()));
        group->add_key("even", get::simple_value_t::create(key("countDict.even")));
        group->add_key("three", get::simple_value_t::create(key("countDict.three")));
        group->add_key("five", get::simple_value_t::create(key("countDict.five")));

        group->add_value("count", std::make_unique<aggregate::operator_count_t>(d(collection)->view()));
        group->add_value("sum", std::make_unique<aggregate::operator_sum_t>(d(collection)->view(), key("count")));
        group->add_value("avg", std::make_unique<aggregate::operator_avg_t>(d(collection)->view(), key("count")));

        auto sort = std::make_unique<operator_sort_t>(d(collection)->view());
        sort->set_children(std::move(group));
        sort->add({"even", "three", "five"});
        sort->on_execute(nullptr);
        REQUIRE(sort->output()->size() == 8);

        document_view_t view0(sort->output()->documents().at(0));
        REQUIRE(view0.get_long("count") == 26);
        REQUIRE(view0.get_long("sum") == 1268);
        REQUIRE(std::fabs(view0.get_double("avg") - 48.77) < 0.01);

        document_view_t view1(sort->output()->documents().at(1));
        REQUIRE(view1.get_long("count") == 7);
        REQUIRE(view1.get_long("sum") == 365);
        REQUIRE(std::fabs(view1.get_double("avg") - 52.14) < 0.01);
    }
}
