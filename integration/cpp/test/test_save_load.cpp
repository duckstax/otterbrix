#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <services/disk/disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal.hpp>

constexpr uint count_databases = 2;
constexpr uint count_collections = 4;
constexpr uint count_documents = 8;

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using namespace components::cursor;
using components::expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

uint gen_doc_number(uint n_db, uint n_col, uint n_doc) { return 10000 * n_db + 100 * n_col + n_doc; }

cursor_t_ptr find_doc(otterbrix::wrapper_dispatcher_t* dispatcher,
                      impl::base_document* tape,
                      const database_name_t& db_name,
                      const collection_name_t& col_name,
                      int n_doc) {
    auto new_value = [&](auto value) { return value_t{tape, value}; };
    auto session_doc = otterbrix::session_id_t();
    auto aggregate = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
    auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{"_id"},
                                                                 id_par{1});
    aggregate->append_child(
        components::logical_plan::make_node_match(dispatcher->resource(), {db_name, col_name}, std::move(expr)));
    auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
    params->add_parameter(id_par{1}, new_value(gen_id(n_doc, dispatcher->resource())));
    auto cur = dispatcher->find_one(session_doc, aggregate, params);
    if (cur->is_success()) {
        cur->next();
    }
    return cur;
}

TEST_CASE("integration::cpp::test_save_load::disk") {
    auto config = test_create_config("/tmp/test_save_load/disk");

    SECTION("initialization") {
        auto resource = std::pmr::synchronized_pool_resource();
        test_clear_directory(config);
        services::disk::disk_t disk(config.disk.path, &resource);
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            disk.append_database(db_name);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                disk.append_collection(db_name, col_name);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    auto doc = gen_doc(int(n_doc), &resource);
                    doc->set("number", gen_doc_number(n_db, n_col, n_doc));
                    disk.save_document(db_name, col_name, doc);
                }
            }
        }
    }

    SECTION("load") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto tape = std::make_unique<impl::base_document>(dispatcher->resource());
        dispatcher->load();
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto session = otterbrix::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto size = dispatcher->size(session, db_name, col_name);
                REQUIRE(size == count_documents);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    REQUIRE(
                        find_doc(dispatcher, tape.get(), db_name, col_name, int(n_doc))->get()->get_ulong("number") ==
                        gen_doc_number(n_db, n_col, n_doc));
                }
            }
        }
    }
}

TEST_CASE("integration::cpp::test_save_load::disk+wal") {
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
                    auto doc = gen_doc(int(n_doc), dispatcher->resource());
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
        auto tape = std::make_unique<impl::base_document>(dispatcher->resource());
        auto new_value = [&](auto value) { return value_t{tape.get(), value}; };
        auto log = initialization_logger("python", config.log.path.c_str());
        log.set_level(config.log.level);
        auto manager = actor_zeta::spawn_supervisor<services::wal::manager_wal_replicate_t>(dispatcher->resource(),
                                                                                            nullptr,
                                                                                            config.wal,
                                                                                            log);
        services::wal::wal_replicate_t wal(manager.get(), log, config.wal);
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                uint n_doc = count_documents + 1;
                auto doc = gen_doc(int(n_doc), dispatcher->resource());
                doc->set("number", gen_doc_number(n_db, n_col, n_doc));
                auto session = otterbrix::session_id_t();
                auto address = actor_zeta::address_t::empty_address();
                auto insert_one =
                    components::logical_plan::make_node_insert(dispatcher->resource(), {db_name, col_name}, doc);
                wal.insert_one(session, address, insert_one);

                {
                    auto match = components::logical_plan::make_node_match(
                        dispatcher->resource(),
                        {db_name, col_name},
                        components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         core::parameter_id_t{1}));
                    auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
                    params->add_parameter(core::parameter_id_t{1}, new_value(1));
                    auto delete_one = components::logical_plan::make_node_delete_one(dispatcher->resource(),
                                                                                     {db_name, col_name},
                                                                                     match);
                    wal.delete_one(session, address, delete_one, params);
                }

                {
                    auto expr = components::expressions::make_compare_union_expression(dispatcher->resource(),
                                                                                       compare_type::union_and);
                    expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                        compare_type::gte,
                                                                                        key{"count"},
                                                                                        core::parameter_id_t{1}));
                    expr->append_child(components::expressions::make_compare_expression(dispatcher->resource(),
                                                                                        compare_type::lte,
                                                                                        key{"count"},
                                                                                        core::parameter_id_t{2}));
                    auto match = components::logical_plan::make_node_match(dispatcher->resource(),
                                                                           {db_name, col_name},
                                                                           std::move(expr));
                    auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
                    params->add_parameter(core::parameter_id_t{1}, new_value(2));
                    params->add_parameter(core::parameter_id_t{2}, new_value(4));
                    auto delete_many = components::logical_plan::make_node_delete_many(dispatcher->resource(),
                                                                                       {db_name, col_name},
                                                                                       match);
                    wal.delete_many(session, address, delete_many, params);
                }

                {
                    auto match = components::logical_plan::make_node_match(
                        dispatcher->resource(),
                        {db_name, col_name},
                        components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         core::parameter_id_t{1}));
                    auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
                    params->add_parameter(core::parameter_id_t{1}, new_value(5));
                    auto update_one = components::logical_plan::make_node_update_one(
                        dispatcher->resource(),
                        {db_name, col_name},
                        match,
                        components::document::document_t::document_from_json(R"({"$set": {"count": 0}})",
                                                                             dispatcher->resource()),
                        false);
                    wal.update_one(session, address, update_one, params);
                }

                {
                    auto match = components::logical_plan::make_node_match(
                        dispatcher->resource(),
                        {db_name, col_name},
                        components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::gt,
                                                                         key{"count"},
                                                                         core::parameter_id_t{1}));
                    auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
                    params->add_parameter(core::parameter_id_t{1}, new_value(5));
                    auto update_many = components::logical_plan::make_node_update_many(
                        dispatcher->resource(),
                        {db_name, col_name},
                        match,
                        components::document::document_t::document_from_json(R"({"$set": {"count": 1000}})",
                                                                             dispatcher->resource()),
                        false);
                    wal.update_many(session, address, update_many, params);
                }
            }
        }
    }

    SECTION("load") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto tape = std::make_unique<impl::base_document>(dispatcher->resource());
        dispatcher->load();
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto session = otterbrix::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto size = dispatcher->size(session, db_name, col_name);
                REQUIRE(size == count_documents - 3);

                REQUIRE_FALSE(find_doc(dispatcher, tape.get(), db_name, col_name, 1)->is_success());
                REQUIRE_FALSE(find_doc(dispatcher, tape.get(), db_name, col_name, 2)->is_success());
                REQUIRE_FALSE(find_doc(dispatcher, tape.get(), db_name, col_name, 3)->is_success());
                REQUIRE_FALSE(find_doc(dispatcher, tape.get(), db_name, col_name, 4)->is_success());

                REQUIRE(find_doc(dispatcher, tape.get(), db_name, col_name, 5)->get()->get_ulong("count") == 0);

                for (uint n_doc = 6; n_doc <= count_documents + 1; ++n_doc) {
                    auto doc_find = find_doc(dispatcher, tape.get(), db_name, col_name, int(n_doc));
                    REQUIRE(doc_find->get()->get_ulong("number") == gen_doc_number(n_db, n_col, n_doc));
                    REQUIRE(doc_find->get()->get_ulong("count") == 1000);
                }
            }
        }
    }
}
