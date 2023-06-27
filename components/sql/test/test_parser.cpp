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
