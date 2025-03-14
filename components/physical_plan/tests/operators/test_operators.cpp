#include "test_operator_generaty.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/index/single_field_index.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/predicates/predicate.hpp>
#include <components/physical_plan/collection/operators/scan/full_scan.hpp>
#include <components/physical_plan/collection/operators/scan/index_scan.hpp>
#include <components/physical_plan/collection/operators/scan/transfer_scan.hpp>

using namespace components::expressions;
using namespace services::collection::operators;
using key = components::expressions::key_t;
using components::logical_plan::add_parameter;

TEST_CASE("operator::insert") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);
    REQUIRE(d(collection)->storage().size() == 100);
}

TEST_CASE("operator::full_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };
    auto collection = init_collection(&resource);

    SECTION("find::eq") {
        auto cond = make_compare_expression(&resource, compare_type::eq, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(&resource, compare_type::ne, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(&resource, compare_type::gte, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(&resource, compare_type::lt, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(&resource, compare_type::lte, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        full_scan scan(d(collection),
                       predicates::create_predicate(d(collection), cond),
                       components::logical_plan::limit_t::limit_one());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("operator::delete") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };
    auto collection = init_collection(&resource);

    SECTION("find::delete") {
        REQUIRE(d(collection)->storage().size() == 100);
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        operator_delete delete_(d(collection));
        delete_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                                predicates::create_predicate(d(collection), cond),
                                                                components::logical_plan::limit_t::unlimit())));
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        delete_.on_execute(&pipeline_context);
        REQUIRE(d(collection)->storage().size() == 90);
    }

    SECTION("find::delete_one") {
        REQUIRE(d(collection)->storage().size() == 100);
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        operator_delete delete_(d(collection));
        delete_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                                predicates::create_predicate(d(collection), cond),
                                                                components::logical_plan::limit_t::limit_one())));
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        delete_.on_execute(&pipeline_context);
        REQUIRE(d(collection)->storage().size() == 99);
    }

    SECTION("find::delete_limit") {
        REQUIRE(d(collection)->storage().size() == 100);
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        operator_delete delete_(d(collection));
        delete_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                                predicates::create_predicate(d(collection), cond),
                                                                components::logical_plan::limit_t(5))));
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        delete_.on_execute(&pipeline_context);
        REQUIRE(d(collection)->storage().size() == 95);
    }
}

TEST_CASE("operator::update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };
    auto collection = init_collection(&resource);

    SECTION("find::update") {
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        add_parameter(parameters, core::parameter_id_t(2), new_value(999));
        components::pipeline::context_t pipeline_context(std::move(parameters));

        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        auto cond_check = make_compare_expression(&resource, compare_type::eq, key("count"), core::parameter_id_t(2));
        auto script_update =
            components::document::document_t::document_from_json(R"({"$set": {"count": 999}})", &resource);
        {
            full_scan scan(d(collection),
                           predicates::create_predicate(d(collection), cond_check),
                           components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection), std::move(script_update), false);
        update_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                                predicates::create_predicate(d(collection), cond),
                                                                components::logical_plan::limit_t::unlimit())));
        update_.on_execute(&pipeline_context);
        {
            full_scan scan(d(collection),
                           predicates::create_predicate(d(collection), cond_check),
                           components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::update_one") {
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        add_parameter(parameters, core::parameter_id_t(2), new_value(999));
        components::pipeline::context_t pipeline_context(std::move(parameters));

        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        auto cond_check = make_compare_expression(&resource, compare_type::eq, key("count"), core::parameter_id_t(2));
        auto script_update =
            components::document::document_t::document_from_json(R"({"$set": {"count": 999}})", &resource);
        {
            full_scan scan(d(collection),
                           predicates::create_predicate(d(collection), cond_check),
                           components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection), std::move(script_update), false);
        update_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                                predicates::create_predicate(d(collection), cond),
                                                                components::logical_plan::limit_t(1))));
        update_.on_execute(&pipeline_context);
        {
            full_scan scan(d(collection),
                           predicates::create_predicate(d(collection), cond_check),
                           components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find::update_limit") {
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        add_parameter(parameters, core::parameter_id_t(2), new_value(999));
        components::pipeline::context_t pipeline_context(std::move(parameters));

        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        auto cond_check = make_compare_expression(&resource, compare_type::eq, key("count"), core::parameter_id_t(2));
        auto script_update =
            components::document::document_t::document_from_json(R"({"$set": {"count": 999}})", &resource);
        {
            full_scan scan(d(collection),
                           predicates::create_predicate(d(collection), cond_check),
                           components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection), std::move(script_update), false);
        update_.set_children(boost::intrusive_ptr(new full_scan(d(collection),
                                                                predicates::create_predicate(d(collection), cond),
                                                                components::logical_plan::limit_t(5))));
        update_.on_execute(&pipeline_context);
        {
            full_scan scan(d(collection),
                           predicates::create_predicate(d(collection), cond_check),
                           components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context);
            REQUIRE(scan.output()->size() == 5);
        }
    }
}

TEST_CASE("operator::index_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };
    auto collection = create_collection(&resource);

    components::index::keys_base_storage_t keys(collection->resource_);
    keys.emplace_back("count");
    components::index::make_index<components::index::single_field_index_t>(d(collection)->index_engine(),
                                                                           "single_count",
                                                                           keys);
    fill_collection(collection);

    SECTION("find::eq") {
        auto cond = make_compare_expression(&resource, compare_type::eq, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = make_compare_expression(&resource, compare_type::ne, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = make_compare_expression(&resource, compare_type::gte, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = make_compare_expression(&resource, compare_type::lt, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = make_compare_expression(&resource, compare_type::lte, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::unlimit());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t::limit_one());
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find_limit") {
        auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        index_scan scan(d(collection), cond, components::logical_plan::limit_t(3));
        components::logical_plan::storage_parameters parameters(&resource);
        add_parameter(parameters, core::parameter_id_t(1), new_value(90));
        components::pipeline::context_t pipeline_context(std::move(parameters));
        scan.on_execute(&pipeline_context);
        REQUIRE(scan.output()->size() == 3);
    }
}

TEST_CASE("operator::transfer_scan") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto collection = init_collection(&resource);

    SECTION("all") {
        transfer_scan scan(d(collection), components::logical_plan::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 100);
    }

    SECTION("limit") {
        transfer_scan scan(d(collection), components::logical_plan::limit_t(50));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 50);
    }

    SECTION("one") {
        transfer_scan scan(d(collection), components::logical_plan::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("operator::index::delete_and_update") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };
    auto collection = create_collection(&resource);

    components::index::keys_base_storage_t keys(collection->resource_);
    keys.emplace_back("count");
    components::index::make_index<components::index::single_field_index_t>(d(collection)->index_engine(),
                                                                           "single_count",
                                                                           keys);
    fill_collection(collection);

    SECTION("index_scan after delete") {
        auto cond_check = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
        components::logical_plan::storage_parameters parameters_check(&resource);
        add_parameter(parameters_check, core::parameter_id_t(1), new_value(50));
        components::pipeline::context_t pipeline_context_check(std::move(parameters_check));
        {
            index_scan scan(d(collection), cond_check, components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 50);
        }
        {
            auto cond = make_compare_expression(&resource, compare_type::gt, key("count"), core::parameter_id_t(1));
            components::logical_plan::storage_parameters parameters(&resource);
            add_parameter(parameters, core::parameter_id_t(1), new_value(60));
            components::pipeline::context_t pipeline_context(std::move(parameters));
            operator_delete delete_(d(collection));
            delete_.set_children(boost::intrusive_ptr(
                new index_scan(d(collection), cond, components::logical_plan::limit_t::unlimit())));
            delete_.on_execute(&pipeline_context);
            REQUIRE(delete_.modified()->size() == 40);

            index_scan scan(d(collection), cond_check, components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("index_scan after update") {
        auto cond_check = make_compare_expression(&resource, compare_type::eq, key("count"), core::parameter_id_t(1));
        components::logical_plan::storage_parameters parameters_check(&resource);
        add_parameter(parameters_check, core::parameter_id_t(1), new_value(50));
        components::pipeline::context_t pipeline_context_check(std::move(parameters_check));
        {
            index_scan scan(d(collection), cond_check, components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 1);
        }
        {
            auto script_update =
                components::document::document_t::document_from_json(R"({"$set": {"count": 0}})", &resource);
            operator_update update(d(collection), script_update, false);
            update.set_children(boost::intrusive_ptr(
                new index_scan(d(collection), cond_check, components::logical_plan::limit_t::unlimit())));
            update.on_execute(&pipeline_context_check);

            index_scan scan(d(collection), cond_check, components::logical_plan::limit_t::unlimit());
            scan.on_execute(&pipeline_context_check);
            REQUIRE(scan.output()->size() == 0);
        }
    }
}
