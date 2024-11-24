#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include "components/tests/generaty.hpp"
#include <integration/cpp/otterbrix.hpp>

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

inline configuration::config make_create_config(const std::filesystem::path& path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.log.level = log_t::level::warn;
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
    otterbrix::otterbrix_ptr otterbrix;

    INFO("initialization") {
        otterbrix = otterbrix::make_otterbrix(config);
        execute_sql(otterbrix, R"_(CREATE DATABASE TestDatabase;)_");
        execute_sql(otterbrix, R"_(CREATE TABLE TestDatabase.TestCollection;)_");
    }

    INFO("insert") {
        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
        for (int num = 0; num < 100; ++num) {
            query << "('" << gen_id(num + 1) << "', "
                  << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
        }
        auto c = execute_sql(otterbrix, query.str());
        REQUIRE(c->size() == 100);
    }

    INFO("select") {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(c->size() == 100);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
    }

    INFO("select order by") {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection ORDER BY count;");
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 0);
            REQUIRE(c->next()->get_long("count") == 1);
            REQUIRE(c->next()->get_long("count") == 2);
            REQUIRE(c->next()->get_long("count") == 3);
            REQUIRE(c->next()->get_long("count") == 4);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection ORDER BY count DESC;");
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 99);
            REQUIRE(c->next()->get_long("count") == 98);
            REQUIRE(c->next()->get_long("count") == 97);
            REQUIRE(c->next()->get_long("count") == 96);
            REQUIRE(c->next()->get_long("count") == 95);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection ORDER BY name;");
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 0);
            REQUIRE(c->next()->get_long("count") == 1);
            REQUIRE(c->next()->get_long("count") == 10);
            REQUIRE(c->next()->get_long("count") == 11);
            REQUIRE(c->next()->get_long("count") == 12);
        }
    }

    INFO("delete") {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
        {
            auto c = execute_sql(otterbrix, "DELETE FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 9);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count > 90;");
            REQUIRE(c->size() == 0);
        }
    }

    INFO("update") {
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            REQUIRE(c->size() == 20);
        }
        {
            auto c = execute_sql(otterbrix, "UPDATE TestDatabase.TestCollection SET count = 1000 WHERE count < 20;");
            REQUIRE(c->size() == 20);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count < 20;");
            REQUIRE(c->size() == 0);
        }
        {
            auto c = execute_sql(otterbrix, "SELECT * FROM TestDatabase.TestCollection WHERE count == 1000;");
            REQUIRE(c->size() == 20);
        }
    }
}

TEST_CASE("example::sql::group_by") {
    auto config = make_create_config("/tmp/test_collection_sql/group_by");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    otterbrix::otterbrix_ptr otterbrix;

    INFO("initialization") {
        otterbrix = otterbrix::make_otterbrix(config);
        execute_sql(otterbrix, R"_(CREATE DATABASE TestDatabase;)_");
        execute_sql(otterbrix, R"_(CREATE TABLE TestDatabase.TestCollection;)_");

        std::stringstream query;
        query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
        for (int num = 0; num < 100; ++num) {
            query << "('" << gen_id(num + 1) << "', "
                  << "'Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
        }
        auto c = execute_sql(otterbrix, query.str());
        REQUIRE(c->is_success());
    }

    INFO("group by") {
        auto c = execute_sql(otterbrix,
                             R"_(SELECT name, COUNT(count) AS count_, )_"
                             R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                             R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                             R"_(FROM TestDatabase.TestCollection )_"
                             R"_(GROUP BY name;)_");
        REQUIRE(c->size() == 10);
        int number = 0;
        while (auto doc = c->next()) {
            REQUIRE(doc->get_string("name") == std::pmr::string("Name " + std::to_string(number)));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            ++number;
        }
    }

    INFO("group by with order by") {
        auto c = execute_sql(otterbrix,
                             R"_(SELECT name, COUNT(count) AS count_, )_"
                             R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                             R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                             R"_(FROM TestDatabase.TestCollection )_"
                             R"_(GROUP BY name )_"
                             R"_(ORDER BY name DESC;)_");
        REQUIRE(c->size() == 10);
        int number = 9;
        while (auto doc = c->next()) {
            REQUIRE(doc->get_string("name") == std::pmr::string("Name " + std::to_string(number)));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            --number;
        }
    }
}

TEST_CASE("example::sql::invalid_queries") {
    auto config = make_create_config("/tmp/test_collection_sql/invalid_queries");
    clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    auto otterbrix = otterbrix::make_otterbrix(config);

    INFO("not exists database") {
        auto c = execute_sql(otterbrix, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == error_code_t::database_not_exists);
    }

    INFO("create database") { execute_sql(otterbrix, R"_(CREATE DATABASE TestDatabase;)_"); }

    INFO("not exists database") {
        auto c = execute_sql(otterbrix, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == error_code_t::collection_not_exists);
    }
}