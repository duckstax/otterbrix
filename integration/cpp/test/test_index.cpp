#include <catch2/catch.hpp>
#include "test_config.hpp"

using components::ql::aggregate::operator_type;
using components::expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

TEST_CASE("integration::test_index") {
    auto config = test_create_config("/tmp/ottergon/integration/test_index");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            components::ql::create_index_t ql{database_name, collection_name, components::ql::index_type::single};
            ql.keys_.emplace_back("count");
            dispatcher->create_index(session, ql);
        }
    }

    INFO("insert") {
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
        for (int num = 1; num <= 100; ++num) {
            documents.push_back(gen_doc(num));
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->insert_many(session, database_name, collection_name, documents);
        }
    }

    INFO("find") {
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 100);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::eq, key{"count"}, id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 1);
            REQUIRE(c->get(0)->get_long("count") == 10);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::gt, key{"count"}, id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 90);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::lt, key{"count"}, id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 9);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::ne, key{"count"}, id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 99);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::gte, key{"count"}, id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 91);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto *ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::lte, key{"count"}, id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto c = dispatcher->find(session, ql);
            REQUIRE(c->size() == 10);
            delete c;
        }
    }

}
