#include <catch2/catch.hpp>
#include <services/disk/disk.hpp>
#include <services/wal/wal.hpp>
#include "test_config.hpp"

constexpr uint count_databases = 2;
constexpr uint count_collections = 5;
constexpr uint count_documents = 10;

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

uint gen_doc_number(uint n_db, uint n_col, uint n_doc) {
    return 10000 * n_db + 100 * n_col + n_doc;
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
                auto session = duck_charmer::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto size = dispatcher->size(session, db_name, col_name);
                REQUIRE(*size == count_documents);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    auto session_doc = duck_charmer::session_id_t();
                    auto doc_find = dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(int(n_doc))));
                    REQUIRE(doc_find->get_ulong("number") == gen_doc_number(n_db, n_col, n_doc));
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
            auto session_db = duck_charmer::session_id_t();
            dispatcher->create_database(session_db, db_name);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto session_col = duck_charmer::session_id_t();
                dispatcher->create_collection(session_col, db_name, col_name);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    auto doc = gen_doc(int(n_doc));
                    doc->set("number", gen_doc_number(n_db, n_col, n_doc));
                    auto session_doc = duck_charmer::session_id_t();
                    dispatcher->insert_one(session_doc, db_name, col_name, doc);
                }
            }
        }
        usleep(100000); //todo: replace by shutdown
    }

    SECTION("extending wal") {
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
                auto session = duck_charmer::session_id_t();
                auto address = actor_zeta::address_t::empty_address();
                components::ql::insert_one_t insert_one(db_name, col_name, doc);
                wal.insert_one(session, address, insert_one);

                components::ql::delete_one_t delete_one(db_name, col_name, components::document::document_from_json(R"({"count": {"$eq": 1}})"));
                wal.delete_one(session, address, delete_one);

                components::ql::delete_many_t delete_many(db_name, col_name, components::document::document_from_json(R"({"count": {"$and": [{"$gte": 2}, {"$lte": 4}]}})"));
                wal.delete_many(session, address, delete_many);

                components::ql::update_one_t update_one(db_name, col_name, components::document::document_from_json(R"({"count": {"$eq": 5}})"),
                                                        components::document::document_from_json(R"({"$set": {"count": 0}})"), false);
                wal.update_one(session, address, update_one);

                components::ql::update_many_t update_many(db_name, col_name, components::document::document_from_json(R"({"count": {"$gt": 5}})"),
                                                          components::document::document_from_json(R"({"$set": {"count": 1000}})"), false);
                wal.update_many(session, address, update_many);
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
                auto session = duck_charmer::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto size = dispatcher->size(session, db_name, col_name);
                REQUIRE(*size == count_documents - 3);
                auto session_doc = duck_charmer::session_id_t();

                REQUIRE_FALSE(dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(1))).is_find());

                REQUIRE_FALSE(dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(2))).is_find());
                REQUIRE_FALSE(dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(3))).is_find());
                REQUIRE_FALSE(dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(4))).is_find());

                session_doc = duck_charmer::session_id_t();
                REQUIRE(dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(5)))->get_ulong("count") == 0);

                for (uint n_doc = 6; n_doc <= count_documents + 1; ++n_doc) {
                    session_doc = duck_charmer::session_id_t();
                    auto doc_find = dispatcher->find_one(session_doc, db_name, col_name, make_condition("_id", "$eq", gen_id(int(n_doc))));
                    REQUIRE(doc_find->get_ulong("number") == gen_doc_number(n_db, n_col, n_doc));
                    REQUIRE(doc_find->get_ulong("count") == 1000);
                }
            }
        }
    }

}
