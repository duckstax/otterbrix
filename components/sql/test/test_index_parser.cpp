#include <catch2/catch.hpp>
#include <components/ql/index.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::create_index") {
    auto* resource = std::pmr::get_default_resource();

    SECTION("create base index") {
        auto ql = sql::parse(resource, "CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION (count)").ql;
        REQUIRE(std::get<ql::create_index_t>(ql).database_ == "TEST_DATABASE");
        REQUIRE(std::get<ql::create_index_t>(ql).collection_ == "TEST_COLLECTION");
        REQUIRE(std::get<ql::create_index_t>(ql).name_ == "base");
        REQUIRE(std::get<ql::create_index_t>(ql).keys_.size() == 1);
        REQUIRE(std::get<ql::create_index_t>(ql).name() == "TEST_COLLECTION_base");
        REQUIRE(std::get<ql::create_index_t>(ql).index_type_ == ql::index_type::single);
        REQUIRE(std::get<ql::create_index_t>(ql).index_compare_ == ql::index_compare::undef);
    }

    SECTION("drop base index by path name") {
        auto ql = sql::parse(resource, "DROP INDEX TEST_DATABASE.TEST_COLLECTION.counter").ql;
        REQUIRE(std::get<ql::drop_index_t>(ql).database_ == "TEST_DATABASE");
        REQUIRE(std::get<ql::drop_index_t>(ql).collection_ == "TEST_COLLECTION");
        REQUIRE(std::get<ql::drop_index_t>(ql).name_ == "counter");
    }
}
