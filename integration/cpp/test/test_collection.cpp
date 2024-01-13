#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

using components::expressions::compare_type;
using components::ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

TEST_CASE("python::test_collection") {
    auto config = test_create_config("/tmp/test_collection");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    dispatcher->load();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 0);
        }
    }

    INFO("one_insert") {
        for (int num = 0; num < 50; ++num) {
            {
                auto doc = gen_doc(num);
                auto session = otterbrix::session_id_t();
                dispatcher->insert_one(session, database_name, collection_name, doc);
            }
            {
                auto session = otterbrix::session_id_t();
                REQUIRE(dispatcher->size(session, database_name, collection_name) == static_cast<std::size_t>(num) + 1);
            }
        }
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->size(session, database_name, collection_name) == 50);
    }

    INFO("many_insert") {
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
        for (int num = 50; num < 100; ++num) {
            documents.push_back(gen_doc(num));
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->insert_many(session, database_name, collection_name, documents);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("insert non unique id") {
        for (int num = 0; num < 100; ++num) {
            {
                auto doc = gen_doc(num);
                auto session = otterbrix::session_id_t();
                dispatcher->insert_one(session, database_name, collection_name, doc);
            }
            {
                auto session = otterbrix::session_id_t();
                REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
            }
        }
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto cur = dispatcher->find(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 90);
            auto cur = dispatcher->find(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }

        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::regex,
                                                                         key{"countStr"},
                                                                         id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, std::string_view{"9$"});
            auto cur = dispatcher->find(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }

        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_or);
            expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::gt,
                                                                                key{"count"},
                                                                                id_par{1}));
            expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::regex,
                                                                                key{"countStr"},
                                                                                id_par{2}));
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 90);
            ql->add_parameter(id_par{2}, std::string_view{"9$"});
            auto cur = dispatcher->find(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 18);
        }

        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr_and =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
            auto expr_or =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_or);
            expr_or->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                   compare_type::gt,
                                                                                   key{"count"},
                                                                                   id_par{1}));
            expr_or->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                   compare_type::regex,
                                                                                   key{"countStr"},
                                                                                   id_par{2}));
            expr_and->append_child(expr_or);
            expr_and->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                    compare_type::lte,
                                                                                    key{"count"},
                                                                                    id_par{3}));
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr_and)));
            ql->add_parameter(id_par{1}, 90);
            ql->add_parameter(id_par{2}, std::string_view{"9$"});
            ql->add_parameter(id_par{3}, 30);
            auto cur = dispatcher->find(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }
    INFO("cursor") {
        auto session = otterbrix::session_id_t();
        auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
        auto cur = dispatcher->find(session, ql);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100);
        int count = 0;
        while (cur->has_next()) {
            cur->next();
            ++count;
        }
        REQUIRE(count == 100);
    }
    INFO("find_one") {
        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"_id"},
                                                                         id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, gen_id(1));
            auto cur = dispatcher->find_one(session, ql);
            REQUIRE(cur->next()->get_long("count") == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         id_par{1});
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 10);
            auto cur = dispatcher->find_one(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->next()->get_long("count") == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto* ql = new components::ql::aggregate_statement{database_name, collection_name};
            auto expr =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
            expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::gt,
                                                                                key{"count"},
                                                                                id_par{1}));
            expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::regex,
                                                                                key{"countStr"},
                                                                                id_par{2}));
            ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            ql->add_parameter(id_par{1}, 90);
            ql->add_parameter(id_par{2}, std::string_view{"9$"});
            auto cur = dispatcher->find_one(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->next()->get_long("count") == 99);
        }
    }
    INFO("drop_collection") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->drop_collection(session, database_name, collection_name);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->drop_collection(session, database_name, collection_name);
            REQUIRE(cur->is_error());
        }
    }
}
