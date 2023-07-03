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

TEST_CASE("parser::insert") {

    SECTION("insert into with schema") {
        auto ql = sql::parse("INSERT INTO schema.table (id, name, count) VALUES (1, 'Name', 1);");
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "schema");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
    }

    SECTION("insert into without schema") {
        auto ql = sql::parse("INSERT INTO table (id, name, count) VALUES (1, 'Name', 1);");
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
    }

    SECTION("insert into error 01") {
        auto ql = sql::parse("INSERT INTO 5 (id, name, count) VALUES (1, 'Name', 1);");
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(ql));
    }

    SECTION("insert into error 02") {
        auto ql = sql::parse("INSERT INTO schema. (id, name, count) VALUES (1, 'Name', 1);");
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(ql));
    }

    SECTION("insert into error 03") {
        auto ql = sql::parse("INSERT INTO schema.5 (id, name, count) VALUES (1, 'Name', 1);");
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(ql));
    }

}
