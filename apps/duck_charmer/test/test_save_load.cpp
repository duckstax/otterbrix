#include <catch2/catch.hpp>
#include <boost/filesystem/operations.hpp>
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

TEST_CASE("duck_charmer::test_save_load::disk") {
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
