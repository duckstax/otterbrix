#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/index/single_field_index.hpp>
#include <services/collection/operators/scan/full_scan.hpp>
#include <services/collection/operators/scan/index_scan.hpp>
#include <services/collection/operators/scan/transfer_scan.hpp>
#include <services/collection/operators/operator_delete.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/operator_update.hpp>
#include <services/collection/operators/predicates/predicate.hpp>
#include "test_operator_generaty.hpp"

using namespace components::expressions;
using namespace services::collection::operators;
using key = components::expressions::key_t;
using components::ql::add_parameter;

TEST_CASE("operator::insert") {
    auto collection = init_collection();
    REQUIRE(d(collection)->size_test() == 100);
}

TEST_CASE("operator::full_scan") {
    auto collection = init_collection();

    SECTION("find::eq") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::eq,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::ne,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gte,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::lt,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::lte,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::limit_one());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 1);
    }
}


TEST_CASE("operator::delete") {
    auto collection = init_collection();

    SECTION("find::delete") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::unlimit()));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        delete_.on_execute(&transaction_context);
        REQUIRE(d(collection)->size_test() == 90);
    }

    SECTION("find::delete_one") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::limit_one()));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        delete_.on_execute(&transaction_context);
        REQUIRE(d(collection)->size_test() == 99);
    }

    SECTION("find::delete_limit") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(5)));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        delete_.on_execute(&transaction_context);
        REQUIRE(d(collection)->size_test() == 95);
    }
}


TEST_CASE("operator::update") {
    auto collection = init_collection();

    SECTION("find::update") {
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        add_parameter(parameters, core::parameter_id_t(2), 999);
        planner::transaction_context_t transaction_context(&parameters);

        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        auto cond_check = make_compare_expression(d(collection)->view()->resource(),
                                                  compare_type::eq,
                                                  key("count"),
                                                  core::parameter_id_t(2));
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::unlimit()));
        update_.on_execute(&transaction_context);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::update_one") {
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        add_parameter(parameters, core::parameter_id_t(2), 999);
        planner::transaction_context_t transaction_context(&parameters);

        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        auto cond_check = make_compare_expression(d(collection)->view()->resource(),
                                                  compare_type::eq,
                                                  key("count"),
                                                  core::parameter_id_t(2));
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(1)));
        update_.on_execute(&transaction_context);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find::update_limit") {
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        add_parameter(parameters, core::parameter_id_t(2), 999);
        planner::transaction_context_t transaction_context(&parameters);

        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        auto cond_check = make_compare_expression(d(collection)->view()->resource(),
                                                  compare_type::eq,
                                                  key("count"),
                                                  core::parameter_id_t(2));
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(5)));
        update_.on_execute(&transaction_context);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context);
            REQUIRE(scan.output()->size() == 5);
        }
    }
}


TEST_CASE("operator::index_scan") {
    auto collection = create_collection();
    components::index::keys_base_storage_t keys(collection->resource);
    keys.emplace_back("count");
    components::index::make_index<components::index::single_field_index_t>(d(collection)->view()->index_engine(), keys);
    fill_collection(collection);

    SECTION("find::eq") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::eq,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::ne,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gte,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::lt,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::lte,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::unlimit());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t::limit_one());
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find_limit") {
        auto cond = make_compare_expression(d(collection)->view()->resource(),
                                            compare_type::gt,
                                            key("count"),
                                            core::parameter_id_t(1));
        index_scan scan(d(collection)->view(), cond, predicates::limit_t(3));
        components::ql::storage_parameters parameters;
        add_parameter(parameters, core::parameter_id_t(1), 90);
        planner::transaction_context_t transaction_context(&parameters);
        scan.on_execute(&transaction_context);
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
    keys.emplace_back("count");
    components::index::make_index<components::index::single_field_index_t>(d(collection)->view()->index_engine(), keys);
    fill_collection(collection);

    SECTION("index_scan after delete") {
        auto cond_check = make_compare_expression(d(collection)->view()->resource(),
                                                  compare_type::gt,
                                                  key("count"),
                                                  core::parameter_id_t(1));
        components::ql::storage_parameters parameters_check;
        add_parameter(parameters_check, core::parameter_id_t(1), 50);
        planner::transaction_context_t transaction_context_check(&parameters_check);
        {
            index_scan scan(d(collection)->view(), cond_check, predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context_check);
            REQUIRE(scan.output()->size() == 50);
        }
        {
            auto cond = make_compare_expression(d(collection)->view()->resource(),
                                                compare_type::gt,
                                                key("count"),
                                                core::parameter_id_t(1));
            components::ql::storage_parameters parameters;
            add_parameter(parameters, core::parameter_id_t(1), 60);
            planner::transaction_context_t transaction_context(&parameters);
            operator_delete delete_(d(collection)->view());
            delete_.set_children(std::make_unique<index_scan>(d(collection)->view(), cond, predicates::limit_t::unlimit()));
            delete_.on_execute(&transaction_context);

            index_scan scan(d(collection)->view(), cond_check, predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context_check);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("index_scan after update") {
        auto cond_check = make_compare_expression(d(collection)->view()->resource(),
                                                  compare_type::eq,
                                                  key("count"),
                                                  core::parameter_id_t(1));
        components::ql::storage_parameters parameters_check;
        add_parameter(parameters_check, core::parameter_id_t(1), 50);
        planner::transaction_context_t transaction_context_check(&parameters_check);
        {
            index_scan scan(d(collection)->view(), cond_check, predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context_check);
            REQUIRE(scan.output()->size() == 1);
        }
        {
            auto script_update = components::document::document_from_json(R"({"$set": {"count": 0}})");
            operator_update update(d(collection)->view(), script_update);
            update.set_children(std::make_unique<index_scan>(d(collection)->view(), cond_check, predicates::limit_t::unlimit()));
            update.on_execute(&transaction_context_check);

            index_scan scan(d(collection)->view(), cond_check, predicates::limit_t::unlimit());
            scan.on_execute(&transaction_context_check);
            REQUIRE(scan.output()->size() == 0);
        }
    }
}
