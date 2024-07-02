#include <catch2/catch.hpp>
#include <components/ql/index.hpp>
#include <components/sql/parser.hpp>
#include <core/types.hpp>

using namespace components;

TEST_CASE("parser::create_index") {
    auto* resource = std::pmr::get_default_resource();

    SECTION("base create index") {
        auto ql = sql::parse(resource, "CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION (count);").ql;
        REQUIRE(std::get<ql::create_index_t>(ql).database_ == "TEST_DATABASE");
        REQUIRE(std::get<ql::create_index_t>(ql).collection_ == "TEST_COLLECTION");
        REQUIRE(std::get<ql::create_index_t>(ql).name_ == "base");
        REQUIRE(std::get<ql::create_index_t>(ql).keys_.size() == 1);
        REQUIRE(std::get<ql::create_index_t>(ql).name() == "TEST_COLLECTION_base");
        REQUIRE(std::get<ql::create_index_t>(ql).index_type_ == ql::index_type::single);
        REQUIRE(std::get<ql::create_index_t>(ql).index_compare_ == types::logical_type::UNKNOWN);
    }

    SECTION("unsupported multi index") {
        const auto query = "CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION (count,name);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "name");
        REQUIRE(res.error.mistake().data() == query + 58);
        // TODO check what()
    }

    // // TODO Add more correct EoQ check
    // SECTION("invalid query: missing end of query") {
    //     const auto query = "CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION (count)";
    //     auto res = sql::parse(resource, query);
    //     REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
    //     REQUIRE(res.error.error() == sql::parse_error::syntax_error);
    //     REQUIRE(res.error.mistake() == ";");
    //     REQUIRE(res.error.mistake().data() == query + 62);
    // }

    SECTION("invalid query: missing index name") {
        const auto query = "CREATE INDEX ON TEST_DATABASE.TEST_COLLECTION (count);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "ON");
        REQUIRE(res.error.mistake().data() == query + 13);
    }

    SECTION("invalid query: missing collection name") {
        const auto query = "CREATE INDEX base ON TEST_DATABASE. (count);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "(");
        REQUIRE(res.error.mistake().data() == query + 36);
    }

    SECTION("invalid query: missing database name") {
        const auto query = "CREATE INDEX base ON .TEST_COLLECTION (count);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == ".");
        REQUIRE(res.error.mistake().data() == query + 21);
    }

    SECTION("invalid query: missing brackets") {
        const auto query = "CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION count";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "count");
        REQUIRE(res.error.mistake().data() == query + 51);
    }

    // TODO looks like it works fine. Result treated as unused
    // SECTION("invalid query: missing key word INDEX") {
    //     const auto query = "CREATE base ON TEST_DATABASE.TEST_COLLECTION (count);";
    //     auto res = sql::parse(resource, query);
    //     REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
    //     REQUIRE(res.error.error() == sql::parse_error::syntax_error);
    //     REQUIRE(res.error.mistake() == "base");
    //     REQUIRE(res.error.mistake().data() == query + 8);
    // }

    SECTION("invalid query: missing database and collection") {
        const auto query = "CREATE INDEX base ON (count);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "(");
        REQUIRE(res.error.mistake().data() == query + 21);
    }

    SECTION("invalid query: missing index fields name") {
        const auto query = "CREATE INDEX base ON TEST_DATABASE.TEST_COLLECTION;";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == ";");
        REQUIRE(res.error.mistake().data() == query + 50);
    }
}

TEST_CASE("parser::drop_index") {
    auto* resource = std::pmr::get_default_resource();

    SECTION("base drop index") {
        auto ql = sql::parse(resource, "DROP INDEX TEST_DATABASE.TEST_COLLECTION.counter;").ql;
        REQUIRE(std::get<ql::drop_index_t>(ql).database_ == "TEST_DATABASE");
        REQUIRE(std::get<ql::drop_index_t>(ql).collection_ == "TEST_COLLECTION");
        REQUIRE(std::get<ql::drop_index_t>(ql).name_ == "counter");
    }

    SECTION("base drop index error") {
        auto ql = sql::parse(resource, "DROP INDEX TEST_DATABASE.TEST_COLLECTION;").ql;
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(ql));
    }
}
