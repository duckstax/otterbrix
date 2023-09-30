#include <catch2/catch.hpp>
#include <variant>
#include <components/ql/statements.hpp>
#include "test_config.hpp"

using namespace components;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

class executor_t {
public:
    explicit executor_t(duck_charmer::wrapper_dispatcher_t* dispatcher)
        : dispatcher_(dispatcher) {}

    components::result::result_t execute(const std::string& query) {
        auto session = duck_charmer::session_id_t();
        return dispatcher_->execute_sql(session, query);
    }

private:
    duck_charmer::wrapper_dispatcher_t* dispatcher_;
};


TEST_CASE("integration::cpp::test_join") {

    auto config = test_create_config("/tmp/test_collection_sql/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    executor_t executor(space.dispatcher());

    INFO("initialization") {
        executor.execute("create database db;");
        executor.execute("create table db.col1;");
        executor.execute("create table db.col2;");
        executor.execute("insert into db.col1 (id, name) values "
                         "(1, 'User1'), "
                         "(2, 'User2'), "
                         "(3, 'User3');");
        executor.execute("insert into db.col2 (id, value) values "
                         "(1, 101), "
                         "(2, 201), "
                         "(2, 202), "
                         "(3, 301), "
                         "(3, 302), "
                         "(3, 303);");
    }

    INFO("join") {
        {
            auto res = executor.execute("select * from db.col1 "
                                        "join db.col2 on db.col1.id = db.col2.id;");
        }
    }

}
