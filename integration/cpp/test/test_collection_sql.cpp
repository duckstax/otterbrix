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

TEST_CASE("integration::cpp::test_collection::sql") {

    auto config = test_create_config("/tmp/test_collection_ql");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space;
    space.init(config);
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
