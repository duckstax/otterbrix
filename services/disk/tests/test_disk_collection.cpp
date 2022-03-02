#include <catch2/catch.hpp>
#include <disk/disk.hpp>

const std::string file_db = "/tmp/collections.rdb";
const std::string database_name1 = "test_database1";
const std::string database_name2 = "test_database2";

using namespace services::disk;

TEST_CASE("sync collection from disk") {
    SECTION("save collection into disk") {
        disk_t disk(file_db);
        disk.append_database(database_name1);
        disk.append_database(database_name2);
        for (int num = 1; num <= 100; ++num) {
            if (num % 2 == 0) {
                disk.append_collection(database_name1, "collection_" + std::to_string(num));
            } else {
                disk.append_collection(database_name2, "collection_" + std::to_string(num));
            }
        }
    }

    SECTION("load list collections from disk") {
        disk_t disk(file_db);
        REQUIRE(disk.collections(database_name1).size() == 50);
        REQUIRE(disk.collections(database_name2).size() == 50);
    }

    SECTION("delete collection from disk") {
        disk_t disk(file_db);
        for (int num = 1; num <= 100; ++num) {
            disk.remove_collection(database_name1, "collection_" + std::to_string(num));
            disk.remove_collection(database_name2, "collection_" + std::to_string(num));
        }
        REQUIRE(disk.collections(database_name1).empty());
        REQUIRE(disk.collections(database_name2).empty());
    }
}