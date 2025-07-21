#include <catch2/catch.hpp>

#include "test_operator_generaty.hpp"

#include <components/physical_plan/collection/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/collection/operators/get/simple_value.hpp>
#include <components/physical_plan/collection/operators/operator_group.hpp>
#include <components/physical_plan/collection/operators/operator_sort.hpp>
#include <components/physical_plan/collection/operators/scan/transfer_scan.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/table/operators/get/simple_value.hpp>
#include <components/physical_plan/table/operators/operator_group.hpp>
#include <components/physical_plan/table/operators/operator_sort.hpp>
#include <components/physical_plan/table/operators/scan/transfer_scan.hpp>

using namespace components::expressions;
using namespace services;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("operator::group::base") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("base::all::no_valid") {
        SECTION("documents") {
            collection::operators::operator_group_t group(d(collection));
            group.set_children(boost::intrusive_ptr(
                new collection::operators::transfer_scan(d(collection), components::logical_plan::limit_t::unlimit())));
            group.add_key("id_", collection::operators::get::simple_value_t::create(key("id_")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 0);
        }
        SECTION("table") {
            services::table::operators::operator_group_t group(d(table));
            group.set_children(boost::intrusive_ptr(
                new table::operators::transfer_scan(d(table), components::logical_plan::limit_t::unlimit())));
            group.add_key("id_", table::operators::get::simple_value_t::create(key("id_")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 0);
        }
    }

    SECTION("base::all::id") {
        SECTION("documents") {
            collection::operators::operator_group_t group(d(collection));
            group.set_children(boost::intrusive_ptr(
                new collection::operators::transfer_scan(d(collection), components::logical_plan::limit_t::unlimit())));
            group.add_key("_id", collection::operators::get::simple_value_t::create(key("_id")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 100);
        }
        SECTION("table") {
            services::table::operators::operator_group_t group(d(table));
            group.set_children(boost::intrusive_ptr(
                new table::operators::transfer_scan(d(table), components::logical_plan::limit_t::unlimit())));
            group.add_key("_id", table::operators::get::simple_value_t::create(key("_id")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 100);
        }
    }

    SECTION("base::all::countBool") {
        SECTION("documents") {
            collection::operators::operator_group_t group(d(collection));
            group.set_children(boost::intrusive_ptr(
                new collection::operators::transfer_scan(d(collection), components::logical_plan::limit_t::unlimit())));
            group.add_key("countBool", collection::operators::get::simple_value_t::create(key("countBool")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 2);
        }
        SECTION("table") {
            services::table::operators::operator_group_t group(d(table));
            group.set_children(boost::intrusive_ptr(
                new table::operators::transfer_scan(d(table), components::logical_plan::limit_t::unlimit())));
            group.add_key("countBool", table::operators::get::simple_value_t::create(key("countBool")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 2);
        }
    }

    SECTION("base::all::dict") {
        SECTION("documents") {
            collection::operators::operator_group_t group(d(collection));
            group.set_children(boost::intrusive_ptr(
                new collection::operators::transfer_scan(d(collection), components::logical_plan::limit_t::unlimit())));
            group.add_key("even", collection::operators::get::simple_value_t::create(key("countDict/even")));
            group.add_key("three", collection::operators::get::simple_value_t::create(key("countDict/three")));
            group.add_key("five", collection::operators::get::simple_value_t::create(key("countDict/five")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 8);
        }
        /*
        SECTION("table") {
            services::table::operators::operator_group_t group(d(table));
            group.set_children(
            boost::intrusive_ptr(new table::operators::transfer_scan(d(table), components::logical_plan::limit_t::unlimit())));
            group.add_key("even", table::operators::get::simple_value_t::create(key("countDict/even")));
            group.add_key("three", table::operators::get::simple_value_t::create(key("countDict/three")));
            group.add_key("five", table::operators::get::simple_value_t::create(key("countDict/five")));
            group.on_execute(nullptr);
            REQUIRE(group.output()->size() == 8);
        }
        */
    }
}

TEST_CASE("operator::group::sort") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("sort::all") {
        SECTION("documents") {
            auto group = boost::intrusive_ptr(new collection::operators::operator_group_t(d(collection)));
            group->set_children(boost::intrusive_ptr(
                new collection::operators::transfer_scan(d(collection), components::logical_plan::limit_t::unlimit())));
            group->add_key("even", collection::operators::get::simple_value_t::create(key("countDict/even")));
            group->add_key("three", collection::operators::get::simple_value_t::create(key("countDict/three")));
            group->add_key("five", collection::operators::get::simple_value_t::create(key("countDict/five")));
            auto sort = boost::intrusive_ptr(new collection::operators::operator_sort_t(d(collection)));
            sort->set_children(std::move(group));
            sort->add({"even", "three", "five"});
            sort->on_execute(nullptr);
            REQUIRE(sort->output()->size() == 8);

            auto check = [](const document_ptr& doc, bool is1, bool is2, bool is3) {
                REQUIRE(doc->get_bool("even") == is1);
                REQUIRE(doc->get_bool("three") == is2);
                REQUIRE(doc->get_bool("five") == is3);
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
        /*
        SECTION("table") {
            auto group = boost::intrusive_ptr(new table::operators::operator_group_t(d(table)));
            group->set_children(
                boost::intrusive_ptr(new table::operators::transfer_scan(d(table), components::logical_plan::limit_t::unlimit())));
            group->add_key("even", table::operators::get::simple_value_t::create(key("countDict/even")));
            group->add_key("three", table::operators::get::simple_value_t::create(key("countDict/three")));
            group->add_key("five", table::operators::get::simple_value_t::create(key("countDict/five")));
            auto sort = boost::intrusive_ptr(new table::operators::operator_sort_t(d(table)));
            sort->set_children(std::move(group));
            sort->add({"even", "three", "five"});
            sort->on_execute(nullptr);
            REQUIRE(sort->output()->size() == 8);

            auto check = [](const document_ptr& doc, bool is1, bool is2, bool is3) {
                REQUIRE(doc->get_bool("even") == is1);
                REQUIRE(doc->get_bool("three") == is2);
                REQUIRE(doc->get_bool("five") == is3);
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
        */
    }
}

TEST_CASE("operator::group::all") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    auto table = init_table(&resource);

    SECTION("sort::all") {
        SECTION("documents") {
            auto group = boost::intrusive_ptr(new collection::operators::operator_group_t(d(collection)));
            group->set_children(boost::intrusive_ptr(
                new collection::operators::transfer_scan(d(collection), components::logical_plan::limit_t::unlimit())));
            group->add_key("even", collection::operators::get::simple_value_t::create(key("countDict/even")));
            group->add_key("three", collection::operators::get::simple_value_t::create(key("countDict/three")));
            group->add_key("five", collection::operators::get::simple_value_t::create(key("countDict/five")));

            group->add_value(
                "count",
                boost::intrusive_ptr(new collection::operators::aggregate::operator_count_t(d(collection))));
            group->add_value("sum",
                             boost::intrusive_ptr(
                                 new collection::operators::aggregate::operator_sum_t(d(collection), key("count"))));
            group->add_value("avg",
                             boost::intrusive_ptr(
                                 new collection::operators::aggregate::operator_avg_t(d(collection), key("count"))));

            auto sort = boost::intrusive_ptr(new collection::operators::operator_sort_t(d(collection)));
            sort->set_children(std::move(group));
            sort->add({"even", "three", "five"});
            sort->on_execute(nullptr);
            REQUIRE(sort->output()->size() == 8);

            auto doc0 = sort->output()->documents().at(0);
            REQUIRE(doc0->get_long("count") == 26);
            REQUIRE(doc0->get_long("sum") == 1268);
            REQUIRE(std::fabs(doc0->get_double("avg") - 48.77) < 0.01);

            auto doc1 = sort->output()->documents().at(1);
            REQUIRE(doc1->get_long("count") == 7);
            REQUIRE(doc1->get_long("sum") == 365);
            REQUIRE(std::fabs(doc1->get_double("avg") - 52.14) < 0.01);
        }
        /*
        SECTION("table") {
            auto group = boost::intrusive_ptr(new table::operators::operator_group_t(d(table)));
            group->set_children(
                boost::intrusive_ptr(new table::operators::transfer_scan(d(table), components::logical_plan::limit_t::unlimit())));
            group->add_key("even", table::operators::get::simple_value_t::create(key("countDict/even")));
            group->add_key("three", table::operators::get::simple_value_t::create(key("countDict/three")));
            group->add_key("five", table::operators::get::simple_value_t::create(key("countDict/five")));

            group->add_value("count", boost::intrusive_ptr(new table::operators::aggregate::operator_count_t(d(table))));
            group->add_value("sum", boost::intrusive_ptr(new table::operators::aggregate::operator_sum_t(d(table), key("count"))));
            group->add_value("avg", boost::intrusive_ptr(new table::operators::aggregate::operator_avg_t(d(table), key("count"))));

            auto sort = boost::intrusive_ptr(new table::operators::operator_sort_t(d(table)));
            sort->set_children(std::move(group));
            sort->add({"even", "three", "five"});
            sort->on_execute(nullptr);
            REQUIRE(sort->output()->size() == 8);

            auto doc0 = sort->output()->documents().at(0);
            REQUIRE(doc0->get_long("count") == 26);
            REQUIRE(doc0->get_long("sum") == 1268);
            REQUIRE(std::fabs(doc0->get_double("avg") - 48.77) < 0.01);

            auto doc1 = sort->output()->documents().at(1);
            REQUIRE(doc1->get_long("count") == 7);
            REQUIRE(doc1->get_long("sum") == 365);
            REQUIRE(std::fabs(doc1->get_double("avg") - 52.14) < 0.01);
        }
        */
    }
}
