#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::invalid") {
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("INVALID QUERY")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("CREATE DATABASE;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DROP DATABASE;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("CREATE TABLE;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DROP TABLE;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("SELECT * FROM;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("INSERT INTO;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DELETE FROM;")));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("UPDATE table_name TO;")));
}

TEST_CASE("parser::database") {

    SECTION("create") {
        auto ql = sql::parse("CREATE DATABASE db_name");
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create;") {
        auto ql = sql::parse("CREATE DATABASE db_name;");
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; ") {
        auto ql = sql::parse("CREATE DATABASE db_name;          ");
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; --") {
        auto ql = sql::parse("CREATE DATABASE db_name; -- comment");
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /*") {
        auto ql = sql::parse("CREATE DATABASE db_name; /* multiline\n"
                             "comments */");
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /* mid */") {
        auto ql = sql::parse("CREATE /* comment */ DATABASE db_name;");
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("drop") {
        auto ql = sql::parse("DROP DATABASE db_name");
        REQUIRE(std::holds_alternative<ql::drop_database_t>(ql));
        REQUIRE(std::get<ql::drop_database_t>(ql).database_ == "db_name");
    }

}
