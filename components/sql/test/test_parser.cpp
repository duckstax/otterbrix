#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::invalid") {
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("INVALID QUERY").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("CREATE DATABASE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DROP DATABASE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("CREATE TABLE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DROP TABLE;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("SELECT * FROM;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("INSERT INTO;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DELETE FROM;").ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("UPDATE table_name TO;").ql));
}

TEST_CASE("parser::database") {

    SECTION("create") {
        auto ql = sql::parse("CREATE DATABASE db_name").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create;") {
        auto ql = sql::parse("CREATE DATABASE db_name;").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; ") {
        auto ql = sql::parse("CREATE DATABASE db_name;          ").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; --") {
        auto ql = sql::parse("CREATE DATABASE db_name; -- comment").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /*") {
        auto ql = sql::parse("CREATE DATABASE db_name; /* multiline\n"
                             "comments */").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /* mid */") {
        auto ql = sql::parse("CREATE /* comment */ DATABASE db_name;").ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("drop") {
        auto ql = sql::parse("DROP DATABASE db_name").ql;
        REQUIRE(std::holds_alternative<ql::drop_database_t>(ql));
        REQUIRE(std::get<ql::drop_database_t>(ql).database_ == "db_name");
    }

}

TEST_CASE("parser::insert") {

    SECTION("insert into with schema") {
        auto ql = sql::parse("INSERT INTO schema.table (id, name, count) VALUES (1, 'Name', 1);").ql;
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "schema");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
    }

    SECTION("insert into without schema") {
        auto ql = sql::parse("INSERT INTO table (id, name, count) VALUES (1, 'Name', 1);").ql;
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
    }

    SECTION("insert into error 01") {
        auto query = "INSERT INTO 5 (id, name, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "5");
        REQUIRE(res.error.mistake().data() == query + 12);
    }

    SECTION("insert into error 02") {
        auto query = "INSERT INTO schema. (id, name, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == " ");
        REQUIRE(res.error.mistake().data() == query + 19);
    }

    SECTION("insert into error 03") {
        auto query = "INSERT INTO schema.5 (id, name, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "5");
        REQUIRE(res.error.mistake().data() == query + 19);
    }

}
