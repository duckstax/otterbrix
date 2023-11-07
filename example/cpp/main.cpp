#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include <integration/cpp/ottergon.hpp>
#include "components/tests/generaty.hpp"

using namespace components;
using namespace components::result;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

inline configuration::config make_create_config(const std::filesystem::path& path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

inline void clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

TEST_CASE("example::sql::base") {
    auto config = make_create_config("/tmp/test_collection_sql/base");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    ottergon::ottergon_ptr ottergon;

    INFO("initialization") { ottergon = ottergon::make_ottergon(config); }

    INFO("insert") {
        {
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto res = execute_sql(ottergon, query.str());
            auto r = res.get<result_insert>();
            REQUIRE(r.inserted_ids().size() == 100);
        }
    }

    INFO("select") {
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            delete c;
        }
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 9);
            delete c;
        }
    }

    INFO("select order by") {
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection ORDER BY count;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 0);
            REQUIRE(c->next()->get_long("count") == 1);
            REQUIRE(c->next()->get_long("count") == 2);
            REQUIRE(c->next()->get_long("count") == 3);
            REQUIRE(c->next()->get_long("count") == 4);
            delete c;
        }
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection ORDER BY count DESC;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 99);
            REQUIRE(c->next()->get_long("count") == 98);
            REQUIRE(c->next()->get_long("count") == 97);
            REQUIRE(c->next()->get_long("count") == 96);
            REQUIRE(c->next()->get_long("count") == 95);
            delete c;
        }
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection ORDER BY name;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 0);
            REQUIRE(c->next()->get_long("count") == 1);
            REQUIRE(c->next()->get_long("count") == 10);
            REQUIRE(c->next()->get_long("count") == 11);
            REQUIRE(c->next()->get_long("count") == 12);
            delete c;
        }
    }

    INFO("delete") {
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 9);
            delete c;
        }
        {
            auto res = execute_sql(ottergon, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            auto r = res.get<result_delete>();
            REQUIRE(r.deleted_ids().size() == 9);
        }
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 0);
            delete c;
        }
    }

    INFO("update") {
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 20);
            delete c;
        }
        {
            auto res = execute_sql(ottergon, "UPDATE TestDatabase.TestCollection SET count = 1000 WHERE count < 20;");
            auto r = res.get<result_update>();
            REQUIRE(r.modified_ids().size() == 20);
        }
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 0);
            delete c;
        }
        {
            auto res = execute_sql(ottergon, "SELECT * FROM TestDatabase.TestCollection WHERE count == 1000;");
            auto* c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 20);
            delete c;
        }
    }
}

TEST_CASE("example::sql::group_by") {
    auto config = make_create_config("/tmp/test_collection_sql/group_by");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    ottergon::ottergon_ptr ottergon;

    INFO("initialization") {
        { ottergon = ottergon::make_ottergon(config); }
        {
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', "
                      << "'Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
            }
            execute_sql(ottergon, query.str());
        }
    }

    INFO("group by") {
        auto res = execute_sql(ottergon, R"_(SELECT name, COUNT(count) AS count_, )_"
                                         R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                         R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                         R"_(FROM TestDatabase.TestCollection )_"
                                         R"_(GROUP BY name;)_");
        auto* c = res.get<components::cursor::cursor_t*>();
        REQUIRE(c->size() == 10);
        int number = 0;
        while (auto doc = c->next()) {
            REQUIRE(doc->get_string("name") == "Name " + std::to_string(number));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            ++number;
        }
        delete c;
    }

    INFO("group by with order by") {
        auto res = execute_sql(ottergon, R"_(SELECT name, COUNT(count) AS count_, )_"
                                         R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                         R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                         R"_(FROM TestDatabase.TestCollection )_"
                                         R"_(GROUP BY name )_"
                                         R"_(ORDER BY name DESC;)_");
        auto* c = res.get<components::cursor::cursor_t*>();
        REQUIRE(c->size() == 10);
        int number = 9;
        while (auto doc = c->next()) {
            REQUIRE(doc->get_string("name") == "Name " + std::to_string(number));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            --number;
        }
        delete c;
    }
}

TEST_CASE("example::sql::invalid_queries") {
    auto config = make_create_config("/tmp/test_collection_sql/invalid_queries");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    auto ottergon = ottergon::make_ottergon(config);

    INFO("not exists database") {
        auto res = execute_sql(ottergon, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(res.is_error());
        REQUIRE(res.error_code() == error_code_t::database_not_exists);
    }

    INFO("create database") { execute_sql(ottergon, R"_(CREATE DATABASE TestDatabase;)_"); }

    INFO("not exists database") {
        auto res = execute_sql(ottergon, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(res.is_error());
        REQUIRE(res.error_code() == error_code_t::collection_not_exists);
    }
}