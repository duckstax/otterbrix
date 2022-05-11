#include <catch2/catch.hpp>
#include <boost/filesystem/operations.hpp>
#include <apps/duck_charmer/spaces.hpp>
#include <services/disk/disk.hpp>
#include <components/tests/generaty.hpp>

constexpr uint count_databases = 2;
constexpr uint count_collections = 5;
constexpr uint count_documents = 10;

static const database_name_t database_name = "FriedrichDatabase";
static const collection_name_t collection_name = "FriedrichCollection";

void clear_(const boost::filesystem::path &current_path) {
    boost::filesystem::remove_all(current_path / "disk");
    boost::filesystem::remove(current_path / ".wal");
}

TEST_CASE("duck_charmer::test_save_load::disk") {
    auto current_path = boost::filesystem::current_path();
    clear_(current_path);

    SECTION("initialization") {
        services::disk::disk_t disk(current_path / "disk");
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            disk.append_database(db_name);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                disk.append_collection(db_name, col_name);
                for (uint n_doc = 1; n_doc <= count_documents; ++n_doc) {
                    auto doc = gen_doc(int(n_doc));
                    disk.save_document(db_name, col_name, get_document_id(doc), doc);
                }
            }
        }
    }

    SECTION("load") {
        auto* space = duck_charmer::spaces::get_instance();
        auto* dispatcher = space->dispatcher();
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto session = duck_charmer::session_id_t();
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto size = dispatcher->size(session, db_name, col_name);
                REQUIRE(*size == count_documents);
            }
        }
    }

}
