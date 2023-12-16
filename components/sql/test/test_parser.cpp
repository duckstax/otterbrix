#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::invalid") {
    auto* resource = std::pmr::get_default_resource();

    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "INVALID QUERY").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "CREATE DATABASE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "DROP DATABASE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "CREATE TABLE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "DROP TABLE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "SELECT * FROM;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "INSERT INTO;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "DELETE FROM;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse(resource, "UPDATE table_name TO;").ql));
}

TEST_CASE("parser::database") {
    auto* resource = std::pmr::get_default_resource();

    SECTION("create") {
        auto ql = sql::parse(resource, "CREATE DATABASE db_name").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create;") {
        auto ql = sql::parse(resource, "CREATE DATABASE db_name;").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; ") {
        auto ql = sql::parse(resource, "CREATE DATABASE db_name;          ").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; --") {
        auto ql = sql::parse(resource, "CREATE DATABASE db_name; -- comment").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /*") {
        auto ql = sql::parse(resource,
                             "CREATE DATABASE db_name; /* multiline\n"
                             "comments */")
                      .ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /* mid */") {
        auto ql = sql::parse(resource, "CREATE /* comment */ DATABASE db_name;").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("drop") {
        auto ql = sql::parse(resource, "DROP DATABASE db_name").ql;
        REQUIRE(std::holds_alternative<ql::drop_database_t>(ql));
        REQUIRE(std::get<ql::drop_database_t>(ql).database_ == "db_name");
    }
}
