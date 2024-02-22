#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/ql/statements.hpp>
#include <variant>

using namespace components;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const std::string database_name = "TestDatabase";
static const std::string collection_name_1 = "TestCollection_1";
static const std::string collection_name_2 = "TestCollection_2";

class executor_t {
public:
    explicit executor_t(otterbrix::wrapper_dispatcher_t* dispatcher)
        : dispatcher_(dispatcher) {}

    components::cursor::cursor_t_ptr execute(const std::string& query) {
        auto session = otterbrix::session_id_t();
        return dispatcher_->execute_sql(session, query);
    }

private:
    otterbrix::wrapper_dispatcher_t* dispatcher_;
};

TEST_CASE("integration::cpp::test_join") {
    auto config = test_create_config("/tmp/test_join/base");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    executor_t executor(space.dispatcher());

    INFO("simple_join") {
        {
            auto session = otterbrix::session_id_t();
            executor.execute("CREATE DATABASE " + database_name + ";");
            executor.execute("CREATE TABLE " + database_name + "." + collection_name_1 + ";");
            executor.execute("CREATE TABLE " + database_name + "." + collection_name_2 + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_1 << " (_id, name, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', "
                      << "'Name " << num << "', " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = executor.execute(query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "INSERT INTO " << database_name << "." << collection_name_2 << " (_id, value, key) VALUES ";
            for (int num = 0; num < 100; ++num) {
                query << "('" << gen_id(num + 1) << "', " << num * 10 << ", " << num << ")" << (num == 99 ? ";" : ", ");
            }
            auto cur = executor.execute(query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            std::stringstream query;
            query << "SELECT * FROM " << database_name + "." << collection_name_1 << " JOIN " << database_name << "."
                  << collection_name_2 << " ON " << database_name << "." << collection_name_1 << ".key"
                  << " = " << database_name << "." << collection_name_2 + ".key;";
            auto cur = executor.execute(query.str());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);

            for (int num = 0; num < 100; ++num) {
                REQUIRE(cur->has_next());
                cur->next();
                REQUIRE(cur->get()->get_long("key") == num);
                REQUIRE(cur->get()->get_long("value") == num * 10);
                REQUIRE(cur->get()->get_string("name") == "Name " + std::to_string(num));
            }
        }
    }
}
