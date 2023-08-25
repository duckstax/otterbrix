#include <variant>

#include <components/ql/statements.hpp>
#include <integration/cpp/base_spaces.hpp>

static configuration::config create_config(const std::filesystem::path& path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

class spaces final : public duck_charmer::base_spaces {
public:
    spaces(const configuration::config& config)
        : duck_charmer::base_spaces(config) {}
};


using namespace components;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

int main(int argc, char** argv) {
    auto config = create_config("/tmp/test_collection_ql");
    config.disk.on = false;
    config.wal.on = false;
    spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = duck_charmer::session_id_t();
    auto res = dispatcher->execute_sql(session, "SELECT * FROM TestDatabase.TestCollection;");
    auto* c = res.get<components::cursor::cursor_t*>();
    delete c;
    return 0;
}