#include <catch2/catch.hpp>
#include <apps/duck_charmer/spaces.hpp>
#include <boost/filesystem/operations.hpp>

constexpr uint count_databases = 5;
constexpr uint count_collections = 10;

static const database_name_t database_name = "FriedrichDatabase";
static const collection_name_t collection_name = "FriedrichCollection";

void clear_() {
    boost::filesystem::path current_path = boost::filesystem::current_path();
    boost::filesystem::remove_all(current_path / "disk");
    boost::filesystem::remove(current_path / ".wal");
}

TEST_CASE("duck_charmer::test_save_load") {
    clear_();
    auto* space = duck_charmer::spaces::get_instance();
    auto* dispatcher = space->dispatcher();

    SECTION("initialization") {
        for (uint n_db = 1; n_db <= count_databases; ++n_db) {
            auto db_name = database_name + "_" + std::to_string(n_db);
            {
                auto session = duck_charmer::session_id_t();
                dispatcher->create_database(session, db_name);
            }
            for (uint n_col = 1; n_col <= count_collections; ++n_col) {
                auto col_name = collection_name + "_" + std::to_string(n_col);
                auto session = duck_charmer::session_id_t();
                dispatcher->create_collection(session, db_name, col_name);
            }
        }
    }
}