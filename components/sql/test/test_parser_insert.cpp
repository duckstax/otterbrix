#include <catch2/catch.hpp>
#include <components/document/document_view.hpp>
#include <components/sql/parser.hpp>

using namespace components;

TEST_CASE("parser::insert_into") {
    auto* resource = std::pmr::get_default_resource();

    SECTION("insert into with schema") {
        auto ql = sql::parse(resource, "INSERT INTO schema.table (id, name, count) VALUES (1, 'Name', 1);").ql;
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "schema");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
        REQUIRE(std::get<ql::insert_many_t>(ql).documents_.size() == 1);
        components::document::document_view_t view(std::get<ql::insert_many_t>(ql).documents_.front());
        REQUIRE(view.get_long("id") == 1);
        REQUIRE(view.get_string("name") == "Name");
        REQUIRE(view.get_long("count") == 1);
    }

    SECTION("insert into without schema") {
        auto ql = sql::parse(resource, "INSERT INTO table (id, name, count) VALUES (1, 'Name', 1);").ql;
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
        REQUIRE(std::get<ql::insert_many_t>(ql).documents_.size() == 1);
        components::document::document_view_t view(std::get<ql::insert_many_t>(ql).documents_.front());
        REQUIRE(view.get_long("id") == 1);
        REQUIRE(view.get_string("name") == "Name");
        REQUIRE(view.get_long("count") == 1);
    }

    SECTION("insert into with quoted") {
        auto ql = sql::parse(resource, "INSERT INTO table (id, \"name\", `count`) VALUES (1, 'Name', 1);").ql;
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
        REQUIRE(std::get<ql::insert_many_t>(ql).documents_.size() == 1);
        components::document::document_view_t view(std::get<ql::insert_many_t>(ql).documents_.front());
        REQUIRE(view.get_long("id") == 1);
        REQUIRE(view.get_string("name") == "Name");
        REQUIRE(view.get_long("count") == 1);
    }

    SECTION("insert into multi-documents") {
        auto ql = sql::parse(resource,
                             "INSERT INTO table (id, name, count) VALUES "
                             "(1, 'Name1', 1), "
                             "(2, 'Name2', 2), "
                             "(3, 'Name3', 3), "
                             "(4, 'Name4', 4), "
                             "(5, 'Name5', 5);")
                      .ql;
        REQUIRE(std::holds_alternative<ql::insert_many_t>(ql));
        REQUIRE(std::get<ql::insert_many_t>(ql).database_ == "");
        REQUIRE(std::get<ql::insert_many_t>(ql).collection_ == "table");
        REQUIRE(std::get<ql::insert_many_t>(ql).documents_.size() == 5);
        components::document::document_view_t view1(std::get<ql::insert_many_t>(ql).documents_.front());
        REQUIRE(view1.get_long("id") == 1);
        REQUIRE(view1.get_string("name") == "Name1");
        REQUIRE(view1.get_long("count") == 1);
        components::document::document_view_t view5(std::get<ql::insert_many_t>(ql).documents_.back());
        REQUIRE(view5.get_long("id") == 5);
        REQUIRE(view5.get_string("name") == "Name5");
        REQUIRE(view5.get_long("count") == 5);
    }

    SECTION("insert into error 01") {
        auto query = "INSERT INTO 5 (id, name, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "5");
        REQUIRE(res.error.mistake().data() == query + 12);
    }

    SECTION("insert into error 02") {
        auto query = "INSERT INTO schema. (id, name, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == " ");
        REQUIRE(res.error.mistake().data() == query + 19);
    }

    SECTION("insert into error 03") {
        auto query = "INSERT INTO schema.5 (id, name, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "5");
        REQUIRE(res.error.mistake().data() == query + 19);
    }

    SECTION("insert into error 04") {
        auto query = "INSERT INTO table (id, name count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "count");
        REQUIRE(res.error.mistake().data() == query + 28);
    }

    SECTION("insert into error 05") {
        auto query = "INSERT INTO table (id, 5, count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "5");
        REQUIRE(res.error.mistake().data() == query + 23);
    }

    SECTION("insert into error 06") {
        auto query = "INSERT INTO table (*) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "*");
        REQUIRE(res.error.mistake().data() == query + 19);
    }

    SECTION("insert into error 07") {
        auto query = "INSERT INTO table () VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::empty_fields_list);
        REQUIRE(res.error.mistake() == ")");
        REQUIRE(res.error.mistake().data() == query + 19);
    }

    SECTION("insert into error 08") {
        auto query = "INSERT INTO table (id, name, count) SET VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "SET");
        REQUIRE(res.error.mistake().data() == query + 36);
    }

    SECTION("insert into error 09") {
        auto query = "INSERT INTO table (id, 'name', count) VALUES (1, 'Name', 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "'name'");
        REQUIRE(res.error.mistake().data() == query + 23);
    }

    SECTION("insert into error 10") {
        auto query = "INSERT INTO table (id, name, count) VALUES (1, Name, 1);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == "Name");
        REQUIRE(res.error.mistake().data() == query + 47);
    }

    SECTION("insert into error 11") {
        auto query = "INSERT INTO table (id, name, count) VALUES ();";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::empty_values_list);
        REQUIRE(res.error.mistake() == ")");
        REQUIRE(res.error.mistake().data() == query + 44);
    }

    SECTION("insert into error 12") {
        auto query = "INSERT INTO table (id, name, count) VALUES (1, 'Name', 1, 2);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::not_valid_size_values_list);
        REQUIRE(res.error.mistake() == ";");
        REQUIRE(res.error.mistake().data() == query + 60);
    }

    SECTION("insert into error 13") {
        auto query = "INSERT INTO table (id, name, count) VALUES "
                     "(1, 'Name1', 1), "
                     "(2, 'Name2', 2, 2), "
                     "(3, 'Name3', 3), "
                     "(4, 'Name4', 4), "
                     "(5, 'Name5', 5);";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::not_valid_size_values_list);
        REQUIRE(res.error.mistake() == ",");
        REQUIRE(res.error.mistake().data() == query + 78);
    }

    SECTION("insert into error 14") {
        auto query = "INSERT INTO table (id, name, count) VALUES "
                     "(1, 'Name1', 1), "
                     "(2, 'Name2', 2), "
                     "(3, 'Name3', 3), "
                     "(4, 'Name4', 4), "
                     "(5, 'Name5', 5), ;";
        auto res = sql::parse(resource, query);
        REQUIRE(std::holds_alternative<ql::unused_statement_t>(res.ql));
        REQUIRE(res.error.error() == sql::parse_error::syntax_error);
        REQUIRE(res.error.mistake() == ";");
        REQUIRE(res.error.mistake().data() == query + 128);
    }
}
