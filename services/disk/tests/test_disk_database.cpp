#include <catch2/catch.hpp>
#include <disk/disk.hpp>

const std::string file_db = "/tmp/databases.rdb";

using namespace services::disk;

TEST_CASE("sync database from disk") {
    SECTION("save databases into disk") {
        remove((file_db + "/METADATA").data());
        disk_t disk(file_db);
        for (int num = 1; num <= 10; ++num) {
            REQUIRE(disk.append_database("database_" + std::to_string(num)));
        }
    }

    SECTION("load list databases from disk") {
        disk_t disk(file_db);
        auto databases = disk.databases();
        REQUIRE(databases.size() == 10);
    }

    SECTION("delete database from disk") {
        disk_t disk(file_db);
        for (int num = 1; num <= 10; num += 2) {
            REQUIRE(disk.remove_database("database_" + std::to_string(num)));
        }
        auto databases = disk.databases();
        REQUIRE(databases.size() == 5);
    }
}