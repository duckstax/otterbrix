#include <catch2/catch.hpp>
#include <components/index/single_field_index.hpp>
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/scan/index_scan.hpp>
#include <services/collection/operators/scan/transfer_scan.hpp>
#include <services/collection/operators/operator_delete.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/operator_update.hpp>
#include <services/collection/operators/predicates/predicate.hpp>
#include "test_operator_generaty.hpp"

using namespace services::collection::operators;

TEST_CASE("operator::insert") {
    auto collection = init_collection();
    REQUIRE(d(collection)->size_test() == 100);
}

TEST_CASE("operator::full_scan") {
    auto collection = init_collection();

    SECTION("find::eq") {
        auto cond = parse_find_condition(R"({"count": {"$eq": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = parse_find_condition(R"({"count": {"$ne": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = parse_find_condition(R"({"count": {"$gte": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = parse_find_condition(R"({"count": {"$lt": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = parse_find_condition(R"({"count": {"$lte": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("operator::delete") {
    auto collection = init_collection();

    SECTION("find::delete") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::unlimit()));
        delete_.on_execute(nullptr);
        REQUIRE(d(collection)->size_test() == 90);
    }

    SECTION("find::delete_one") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(1)));
        delete_.on_execute(nullptr);
        REQUIRE(d(collection)->size_test() == 99);
    }

    SECTION("find::delete_limit") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(5)));
        delete_.on_execute(nullptr);
        REQUIRE(d(collection)->size_test() == 95);
    }
}


TEST_CASE("operator::update") {
    auto collection = init_collection();

    SECTION("find::update") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        auto cond_check = parse_find_condition(R"({"count": {"$eq": 999}})");
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::unlimit()));
        update_.on_execute(nullptr);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::update_one") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        auto cond_check = parse_find_condition(R"({"count": {"$eq": 999}})");
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(1)));
        update_.on_execute(nullptr);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find::update_limit") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        auto cond_check = parse_find_condition(R"({"count": {"$eq": 999}})");
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(5)));
        update_.on_execute(nullptr);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 5);
        }
    }
}

TEST_CASE("operator::index_scan") {
    auto collection = create_collection();
    components::index::keys_base_storage_t keys(collection->resource);
    keys.push_back(components::index::key_t("count"));
    components::index::make_index<components::index::single_field_index_t>(d(collection)->view()->index_engine(), keys);
    fill_collection(collection);

    SECTION("find::eq") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$eq": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$ne": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gte": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$lt": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$lte": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 90}})"), predicates::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find_limit") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 90}})"), predicates::limit_t(3));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 3);
    }
}

TEST_CASE("operator::transfer_scan") {
    auto collection = init_collection();

    SECTION("all") {
        transfer_scan scan(d(collection)->view(), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 100);
    }

    SECTION("limit") {
        transfer_scan scan(d(collection)->view(), predicates::limit_t(50));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 50);
    }

    SECTION("one") {
        transfer_scan scan(d(collection)->view(), predicates::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("operator::index::delete_and_update") {
    auto collection = create_collection();
    components::index::keys_base_storage_t keys(collection->resource);
    keys.push_back(components::index::key_t("count"));
    components::index::make_index<components::index::single_field_index_t>(d(collection)->view()->index_engine(), keys);
    fill_collection(collection);

    SECTION("index_scan after delete") {
        {
            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 50);
        }
        {
            operator_delete delete_(d(collection)->view());
            delete_.set_children(std::make_unique<index_scan>(d(collection)->view(),
                                                              parse_expr(R"({"count": {"$gt": 60}})"), predicates::limit_t::unlimit()));
            delete_.on_execute(nullptr);

            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("index_scan after update") {
        {
            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$eq": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 1);
        }
        {
            auto script_update = components::document::document_from_json(R"({"$set": {"count": 0}})");
            operator_update update(d(collection)->view(), script_update);
            update.set_children(std::make_unique<index_scan>(d(collection)->view(),
                                                             parse_expr(R"({"count": {"$eq": 50}})"), predicates::limit_t::unlimit()));
            update.on_execute(nullptr);

            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$eq": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }
    }
}
