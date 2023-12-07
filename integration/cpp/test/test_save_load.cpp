#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <services/disk/disk.hpp>
#include <services/wal/wal.hpp>
#include "test_config.hpp"

constexpr uint count_databases = 2;
constexpr uint count_collections = 4;
constexpr uint count_documents = 8;

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

using namespace components::cursor;
using components::ql::aggregate::operator_type;
using components::expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

uint gen_doc_number(uint n_db, uint n_col, uint n_doc) {
    return 10000 * n_db + 100 * n_col + n_doc;
}

cursor_t_ptr find_doc(otterbrix::wrapper_dispatcher_t* dispatcher,
                         const database_name_t &db_name,
                         const collection_name_t &col_name,
                         int n_doc) {
    auto session_doc = otterbrix::session_id_t();
    auto *ql = new components::ql::aggregate_statement{db_name, col_name};
    auto expr = components::expressions::make_compare_expression(dispatcher->resource(), compare_type::eq, key{"_id"}, id_par{1});
    ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));
    ql->add_parameter(id_par{1}, gen_id(n_doc));
    auto cur = dispatcher->find_one(session_doc, ql);
    if (cur->is_success()) {
        cur->next();
    }
    return cur;
}

TEST_CASE("python::test_save_load::disk") {
    auto config = test_create_config("/tmp/test_save_load/disk");

    SECTION("initialization") {
        test_clear_directory(config);
        services::disk::disk_t disk(config.disk.path);
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            disk.append_database(db_name);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                disk.append_collection(db_name, col_name);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    auto doc = gen_doc(int(n_doc));
                    doc->set("number", gen_doc_number(n_db, n_col, n_doc));
                    disk.save_document(db_name, col_name, get_document_id(doc), doc);
                }
            }
        }
    }

    SECTION("load") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        dispatcher->load();
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto session = otterbrix::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto cur = dispatcher->size(session, db_name, col_name);
                REQUIRE(cur->size() == count_documents);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    REQUIRE(find_doc(dispatcher, db_name, col_name, int(n_doc))->get()->get_ulong("number") == gen_doc_number(n_db, n_col, n_doc));
                }
            }
        }
    }

}


 TEST_CASE("python::test_save_load::disk+wal") {
    auto config = test_create_config("/tmp/test_save_load/wal");

    SECTION("initialization") {
        test_clear_directory(config);
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            auto session_db = otterbrix::session_id_t();
            dispatcher->create_database(session_db, db_name);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto session_col = otterbrix::session_id_t();
                dispatcher->create_collection(session_col, db_name, col_name);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    auto doc = gen_doc(int(n_doc));
                    doc->set("number", gen_doc_number(n_db, n_col, n_doc));
                    auto session_doc = otterbrix::session_id_t();
                    dispatcher->insert_one(session_doc, db_name, col_name, doc);
                }
            }
        }
    }

    SECTION("extending wal") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto log = initialization_logger("python", config.log.path.c_str());
        log.set_level(config.log.level);
        services::wal::wal_replicate_t wal(nullptr, log, config.wal);
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                uint n_doc = count_documents + 1;
                auto doc = gen_doc(int(n_doc));
                doc->set("number", gen_doc_number(n_db, n_col, n_doc));
                auto session = otterbrix::session_id_t();
                auto address = actor_zeta::address_t::empty_address();
                components::ql::insert_one_t insert_one(db_name, col_name, doc);
                wal.insert_one(session, address, insert_one);

                {
                    auto match = components::ql::aggregate::make_match(
                                components::expressions::make_compare_expression(dispatcher->resource(), compare_type::eq, key{"count"}, core::parameter_id_t{1}));
                    components::ql::storage_parameters params;
                    components::ql::add_parameter(params, core::parameter_id_t{1}, 1);
                    components::ql::delete_one_t delete_one(db_name, col_name, match, params);
                    wal.delete_one(session, address, delete_one);
                }

                {
                    auto expr = components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
                    expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(), compare_type::gte, key{"count"}, core::parameter_id_t{1}));
                    expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(), compare_type::lte, key{"count"}, core::parameter_id_t{2}));
                    auto match = components::ql::aggregate::make_match(expr);
                    components::ql::storage_parameters params;
                    components::ql::add_parameter(params, core::parameter_id_t{1}, 2);
                    components::ql::add_parameter(params, core::parameter_id_t{2}, 4);
                    components::ql::delete_many_t delete_many(db_name, col_name, match, params);
                    wal.delete_many(session, address, delete_many);
                }

                {
                    auto match = components::ql::aggregate::make_match(
                                components::expressions::make_compare_expression(dispatcher->resource(), compare_type::eq, key{"count"}, core::parameter_id_t{1}));
                    components::ql::storage_parameters params;
                    components::ql::add_parameter(params, core::parameter_id_t{1}, 5);
                    components::ql::update_one_t update_one(db_name, col_name, match, params, components::document::document_from_json(R"({"$set": {"count": 0}})"), false);
                    wal.update_one(session, address, update_one);
                }

                {
                    auto match = components::ql::aggregate::make_match(
                                components::expressions::make_compare_expression(dispatcher->resource(), compare_type::gt, key{"count"}, core::parameter_id_t{1}));
                    components::ql::storage_parameters params;
                    components::ql::add_parameter(params, core::parameter_id_t{1}, 5);
                    components::ql::update_many_t update_many(db_name, col_name, match, params, components::document::document_from_json(R"({"$set": {"count": 1000}})"), false);
                    wal.update_many(session, address, update_many);
                }
            }
        }
    }

    SECTION("load") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        dispatcher->load();
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto session = otterbrix::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto cur = dispatcher->size(session, db_name, col_name);
                REQUIRE(cur->size() == count_documents - 3);

                REQUIRE_FALSE(find_doc(dispatcher, db_name, col_name, 1)->is_success());
                REQUIRE_FALSE(find_doc(dispatcher, db_name, col_name, 2)->is_success());
                REQUIRE_FALSE(find_doc(dispatcher, db_name, col_name, 3)->is_success());
                REQUIRE_FALSE(find_doc(dispatcher, db_name, col_name, 4)->is_success());

                REQUIRE(find_doc(dispatcher, db_name, col_name, 5)->get()->get_ulong("count") == 0);

                for (uint n_doc = 6; n_doc <= count_documents + 1; ++n_doc) {
                    auto doc_find = find_doc(dispatcher, db_name, col_name, int(n_doc));
                    REQUIRE(doc_find->get()->get_ulong("number") == gen_doc_number(n_db, n_col, n_doc));
                    REQUIRE(doc_find->get()->get_ulong("count") == 1000);
                }
            }
        }
    }
}
