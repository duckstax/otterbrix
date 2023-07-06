#include <catch2/catch.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::invalid") {
    auto* resource = std::pmr::get_default_resource();

    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("INVALID QUERY", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("CREATE DATABASE;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DROP DATABASE;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("CREATE TABLE;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DROP TABLE;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("SELECT * FROM;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("INSERT INTO;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("DELETE FROM;", resource).ql));
    REQUIRE(std::holds_alternative<ql::unused_statement_t>(sql::parse("UPDATE table_name TO;", resource).ql));
}

TEST_CASE("parser::database") {
    auto* resource = std::pmr::get_default_resource();

    SECTION("create") {
        auto ql = sql::parse("CREATE DATABASE db_name", resource).ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create;") {
        auto ql = sql::parse("CREATE DATABASE db_name;", resource).ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; ") {
        auto ql = sql::parse("CREATE DATABASE db_name;          ", resource).ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; --") {
        auto ql = sql::parse("CREATE DATABASE db_name; -- comment", resource).ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /*") {
        auto ql = sql::parse("CREATE DATABASE db_name; /* multiline\n"
                             "comments */", resource).ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("create; /* mid */") {
        auto ql = sql::parse("CREATE /* comment */ DATABASE db_name;", resource).ql;
        REQUIRE(std::holds_alternative<ql::create_database_t>(ql));
        REQUIRE(std::get<ql::create_database_t>(ql).database_ == "db_name");
    }

    SECTION("drop") {
        auto ql = sql::parse("DROP DATABASE db_name", resource).ql;
        REQUIRE(std::holds_alternative<ql::drop_database_t>(ql));
        REQUIRE(std::get<ql::drop_database_t>(ql).database_ == "db_name");
    }

}
