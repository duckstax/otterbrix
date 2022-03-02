#include <catch2/catch.hpp>
#include <disk/metadata.hpp>

using namespace services::disk;

const std::string database_name1 = "test_database1";
const std::string database_name2 = "test_database2";

TEST_CASE("metadata databases") {
    auto metadata = metadata_t::extract("");

    REQUIRE(metadata->databases().empty());
    for (size_t num = 1; num <= 10; ++num) {
        auto database = "database_" + std::to_string(num);
        metadata->append_database(database);
        REQUIRE(metadata->databases().size() == num);
        REQUIRE(metadata->is_exists_database(database));
        metadata->append_database(database);
        REQUIRE(metadata->databases().size() == num);
    }

    REQUIRE(metadata->databases().size() == 10);
    metadata->remove_database("not_valid_database");
    REQUIRE(metadata->databases().size() == 10);
    for (size_t num = 1; num <= 10; ++num) {
        auto database = "database_" + std::to_string(num);
        metadata->remove_database(database);
        REQUIRE(metadata->databases().size() == 10 - num);
        REQUIRE(!metadata->is_exists_database(database));
        metadata->remove_database(database);
        REQUIRE(metadata->databases().size() == 10 - num);
    }
}

TEST_CASE("metadata collections") {
    auto metadata = metadata_t::extract("");
    metadata->append_database(database_name1);
    metadata->append_database(database_name2);
    size_t count1 = 0;
    size_t count2 = 0;

    REQUIRE(metadata->collections(database_name1).empty());
    REQUIRE(metadata->collections(database_name2).empty());
    for (size_t num = 1; num <= 20; ++num) {
        auto collection = "collection_" + std::to_string(num);
        if (num % 2 == 0) {
            ++count1;
            metadata->append_collection(database_name1, collection);
            REQUIRE(metadata->collections(database_name1).size() == count1);
            REQUIRE(metadata->is_exists_collection(database_name1, collection));
            metadata->append_collection(database_name1, collection);
        } else {
            ++count2;
            metadata->append_collection(database_name2, collection);
            REQUIRE(metadata->collections(database_name2).size() == count2);
            REQUIRE(metadata->is_exists_collection(database_name2, collection));
            metadata->append_collection(database_name2, collection);
        }
        REQUIRE(metadata->collections(database_name1).size() == count1);
        REQUIRE(metadata->collections(database_name2).size() == count2);
    }

    REQUIRE(metadata->collections(database_name1).size() == count1);
    REQUIRE(metadata->collections(database_name2).size() == count2);
    metadata->remove_collection(database_name1, "not_valid_collection");
    metadata->remove_collection(database_name2, "not_valid_collection");
    REQUIRE(metadata->collections(database_name1).size() == count1);
    REQUIRE(metadata->collections(database_name2).size() == count2);
    for (size_t num = 1; num <= 20; ++num) {
        auto collection = "collection_" + std::to_string(num);
        if (num % 2 == 0) {
            --count1;
        } else {
            --count2;
        }
        metadata->remove_collection(database_name1, collection);
        metadata->remove_collection(database_name2, collection);
        REQUIRE(metadata->collections(database_name1).size() == count1);
        REQUIRE(metadata->collections(database_name2).size() == count2);
        REQUIRE(!metadata->is_exists_collection(database_name1, collection));
        REQUIRE(!metadata->is_exists_collection(database_name2, collection));
    }
    REQUIRE(metadata->collections(database_name1).empty());
    REQUIRE(metadata->collections(database_name2).empty());
}