#include <catch2/catch.hpp>
#include <variant>
#include <components/ql/statements.hpp>
#include "test_config.hpp"

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

using namespace components;
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
            auto session = duck_charmer::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
    }

    INFO("insert") {
        {
            auto session = duck_charmer::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', " << "'Name " << num << "', "
                      << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto res = dispatcher->execute_sql(session, query.str());
            auto r = res.get<result_insert>();
            REQUIRE(r.inserted_ids().size() == 100);
        }
        {
            auto session = duck_charmer::session_id_t();
            REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("find") {
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "WHERE count > 90;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 9);
            delete c;
        }
    }

    INFO("find order by") {
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "ORDER BY count;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 0);
            REQUIRE(c->next()->get_long("count") == 1);
            REQUIRE(c->next()->get_long("count") == 2);
            REQUIRE(c->next()->get_long("count") == 3);
            REQUIRE(c->next()->get_long("count") == 4);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "ORDER BY count DESC;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 100);
            REQUIRE(c->next()->get_long("count") == 99);
            REQUIRE(c->next()->get_long("count") == 98);
            REQUIRE(c->next()->get_long("count") == 97);
            REQUIRE(c->next()->get_long("count") == 96);
            REQUIRE(c->next()->get_long("count") == 95);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "ORDER BY name;");
            auto *c = res.get<components::cursor::cursor_t*>();
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
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "WHERE count > 90;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 9);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "DELETE FROM TestDatabase.TestCollection "
                                                        "WHERE count > 90;");
            auto r = res.get<result_delete>();
            REQUIRE(r.deleted_ids().size() == 9);
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "WHERE count > 90;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 0);
            delete c;
        }
    }

    INFO("update") {
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "WHERE count < 20;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 20);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "UPDATE TestDatabase.TestCollection "
                                                        "SET count = 1000 "
                                                        "WHERE count < 20;");
            auto r = res.get<result_update>();
            REQUIRE(r.modified_ids().size() == 20);
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "WHERE count < 20;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 0);
            delete c;
        }
        {
            auto session = duck_charmer::session_id_t();
            auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection "
                                                        "WHERE count == 1000;");
            auto *c = res.get<components::cursor::cursor_t*>();
            REQUIRE(c->size() == 20);
            delete c;
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
            auto session = duck_charmer::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            std::stringstream query;
            query << "INSERT INTO TestDatabase.TestCollection (_id, name, count) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', " << "'Name " << (num % 10) << "', "
                      << (num % 20) << ")" << (num == 99 ? ";" : ", ");
            }
            dispatcher->execute_sql(session, query.str());
        }
    }

    INFO("group by") {
        auto session = duck_charmer::session_id_t();
        auto res = dispatcher->execute_sql(session, R"_(SELECT name, COUNT(count) AS count_, )_"
                                                    R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                                    R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                                    R"_(FROM TestDatabase.TestCollection )_"
                                                    R"_(GROUP BY name;)_");
        auto *c = res.get<components::cursor::cursor_t*>();
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
        auto session = duck_charmer::session_id_t();
        auto res = dispatcher->execute_sql(session, R"_(SELECT name, COUNT(count) AS count_, )_"
                                                    R"_(SUM(count) AS sum_, AVG(count) AS avg_, )_"
                                                    R"_(MIN(count) AS min_, MAX(count) AS max_ )_"
                                                    R"_(FROM TestDatabase.TestCollection )_"
                                                    R"_(GROUP BY name )_"
                                                    R"_(ORDER BY name DESC;)_");
        auto *c = res.get<components::cursor::cursor_t*>();
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


TEST_CASE("integration::cpp::test_collection::sql::invalid_queries") {

    auto config = test_create_config("/tmp/test_collection_sql/invalid_queries");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("not exists database") {
        auto session = duck_charmer::session_id_t();
        auto res = dispatcher->execute_sql(session, R"_(SELECT * FROM TestDatabase.TestCollection;)_");
        REQUIRE(res.is_type<components::cursor::cursor_t*>());
        REQUIRE(res.get<components::cursor::cursor_t*>()->size() == 0);
    }

}
