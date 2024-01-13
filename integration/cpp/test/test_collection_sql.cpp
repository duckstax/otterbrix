#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/ql/statements.hpp>
#include <variant>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

TEST_CASE("integration::cpp::test_collection::sql::base") {
    auto config = test_create_config("/tmp/test_collection_sql/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
    }

    INFO("insert") {
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = dispatcher->execute_sql(session, query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
    }

    INFO("find order by") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->next()->get_long("count") == 0);
            REQUIRE(cur->next()->get_long("count") == 1);
            REQUIRE(cur->next()->get_long("count") == 2);
            REQUIRE(cur->next()->get_long("count") == 3);
            REQUIRE(cur->next()->get_long("count") == 4);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY count DESC;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->next()->get_long("count") == 99);
            REQUIRE(cur->next()->get_long("count") == 98);
            REQUIRE(cur->next()->get_long("count") == 97);
            REQUIRE(cur->next()->get_long("count") == 96);
            REQUIRE(cur->next()->get_long("count") == 95);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "ORDER BY name;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
            REQUIRE(cur->next()->get_long("count") == 0);
            REQUIRE(cur->next()->get_long("count") == 1);
            REQUIRE(cur->next()->get_long("count") == 10);
            REQUIRE(cur->next()->get_long("count") == 11);
            REQUIRE(cur->next()->get_long("count") == 12);
        }
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "DELETE FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 9);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count > 90;");
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "UPDATE TestDatabase.TestCollection "
                                               "SET count = 1000 "
                                               "WHERE count < 20;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count < 20;");
            REQUIRE_FALSE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                                               "SELECT * FROM TestDatabase.TestCollection "
                                               "WHERE count == 1000;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 20);
        }
    }
}

TEST_CASE("integration::cpp::test_collection::sql::group_by") {
    auto config = test_create_config("/tmp/test_collection_sql/group_by");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', "
                      << "'Name " << (num % 10) << "', " << (num % 20) << ")" << (num == 99 ? ";" : ", ");
            }
            dispatcher->execute_sql(session, query.str());
        }
    }

    INFO("group by") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT name, COUNT(count) AS count_, )_"
                                           R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                           R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY name;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        int number = 0;
        while (auto doc = cur->next()) {
            REQUIRE(doc->get_string("name") == "Name " + std::to_string(number));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            ++number;
        }
    }

    INFO("group by with order by") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           R"_(SELECT name, COUNT(count) AS count_, )_"
                                           R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                           R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                           R"_(FROM TestDatabase.TestCollection )_"
                                           R"_(GROUP BY name )_"
                                           R"_(ORDER BY name DESC;)_");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 10);
        int number = 9;
        while (auto doc = cur->next()) {
            REQUIRE(doc->get_string("name") == "Name " + std::to_string(number));
            REQUIRE(doc->get_long("count_") == 10);
            REQUIRE(doc->get_long("sum_") == 5 * (number % 20) + 5 * ((number + 10) % 20));
            REQUIRE(doc->get_long("avg_") == (number % 20 + (number + 10) % 20) / 2);
            REQUIRE(doc->get_long("min_") == number % 20);
            REQUIRE(doc->get_long("max_") == (number + 10) % 20);
            --number;
        }
    }
}

TEST_CASE("integration::cpp::test_collection::sql::invalid_queries") {
    auto config = test_create_config("/tmp/test_collection_sql/invalid_queries");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("not exists database") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == error_code_t::database_not_exists);
    }

    INFO("create database") {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, R"_(CREATE DATABASE TestDatabase;)_");
    }

    INFO("not exists database") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == error_code_t::collection_not_exists);
    }
}
