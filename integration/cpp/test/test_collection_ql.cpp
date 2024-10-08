#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/ql/statements.hpp>
#include <variant>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::ql") {
    auto config = test_create_config("/tmp/test_collection_ql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto tape = std::make_unique<impl::base_document>(dispatcher->resource());
    auto new_value = [&](auto value) { return value_t{tape.get(), value}; };

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
    }

    INFO("insert") {
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
        for (int num = 0; num < kNumInserts; ++num) {
            documents.push_back(gen_doc(num, dispatcher->resource()));
        }
        ql::insert_many_t ins{database_name, collection_name, documents};
        {
            auto session = otterbrix::session_id_t();
            components::ql::variant_statement_t ql{ins};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == kNumInserts);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::delete_many_t del{database_name, collection_name, dispatcher->resource()};
            del.match_.query = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::gt,
                                                                                key{"count"},
                                                                                id_par{1});
            del.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{del};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(90));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::lt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(20));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::update_many_t upd{database_name, collection_name, dispatcher->resource()};
            upd.match_.query = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::lt,
                                                                                key{"count"},
                                                                                id_par{1});
            upd.add_parameter(id_par{1}, new_value(20));
            upd.update_ = make_document(dispatcher->resource());
            upd.update_->set_dict("$set");
            upd.update_->get_dict("$set")->set("count", 1000);
            components::ql::variant_statement_t ql{upd};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::lt,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(20));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            components::ql::aggregate_statement agg{database_name, collection_name, dispatcher->resource()};
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         id_par{1});
            agg.append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
            agg.add_parameter(id_par{1}, new_value(1000));
            components::ql::variant_statement_t ql{agg};
            auto cur = dispatcher->execute_ql(session, ql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
    }
}
