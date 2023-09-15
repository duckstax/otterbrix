#include <variant>

#include <components/ql/statements.hpp>
#include <integration/cpp/base_spaces.hpp>
#include <components/result/result.hpp>

static configuration::config_t create_config(const std::filesystem::path& path) {
    auto config = configuration::config_t::default_config();
    config.log.path = path;
    config.log.level = log_t::level::trace;
    config.log.name = "app";
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

class spaces final : public duck_charmer::base_spaces {
public:
    spaces()
        : duck_charmer::base_spaces() {}
};

using namespace components;
using expressions::compare_type;
using ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

template<typename ... Ts>
struct overload : Ts ... {
    using Ts::operator() ...;
};

template<class... Ts> overload(Ts...) -> overload<Ts...>;

void show(components::result::result_t&result){
    result.visit(
        overload{
            [](components::result::error_result_t){},
            [](components::result::empty_result_t) {},
            [](components::result::result_address_t) {},
            [](components::result::result_list_addresses_t) {},
            [](components::cursor::cursor_t* result) {
                std::cerr << "start " << std::endl;
                std::cerr <<" size: " << result->size() << std::endl;
                for ( const auto&i: (*result)) {
                    for(const auto&j:i->data()){
                        std::cout << j.to_json() << std::endl;
                    }
                }
                std::cerr << "stop " << std::endl;

                delete result;
            },
            [](components::result::result_insert result) {
                if (result.empty()) {
                    std::cout << "inserted ids: 0" << std::endl;
                } else {
                    std::cerr << "inserted ids:" << std::endl;
                    for (auto& i : result.inserted_ids()) {
                        std::cout << i.to_string() << std::endl;
                    }
                }
            },
            [](components::result::result_delete result) {
                if (result.empty()) {
                    std::cerr << "inserted ids: 0" << std::endl;
                } else {
                    std::cerr << "inserted ids:" << std::endl;
                    for (auto& i : result.deleted_ids()) {
                        std::cout << i.to_string() << std::endl;
                    }
                }
            },
            [](components::result::result_update result) {
                if (result.empty()) {
                    std::cerr << "inserted ids: 0" << std::endl;
                } else {
                    std::cout << "inserted ids:" << std::endl;
                    for (auto& i : result.modified_ids()) {
                        std::cerr << i.to_string() << std::endl;
                    }
                }
            },
            [](components::result::result_create_index result) {
                if (result.is_success()) {
                    std::cerr << "index created" << std::endl;
                } else {
                    std::cerr << "index not created" << std::endl;
                }
            },
            [](components::result::result_drop_index result) {
                if (result.is_success()) {
                    std::cerr << "index dropped" << std::endl;
                } else {
                    std::cerr << "index not dropped" << std::endl;
                }
            },
            [](components::result::result_drop_collection result) {
                if (result.is_success()) {
                    std::cerr << "collection dropped" << std::endl;
                } else {
                    std::cerr << "collection not dropped" << std::endl;
                }
            }});
}

int main(int argc, char** argv) {
    std::vector<std::string> args(argv + 1, argv + argc);

    auto config = create_config("/tmp/test");
    config.disk.on = false;
    config.wal.on = false;
    config.log.level = log_t::level::trace;
    config.log.name = "app";

    /////
    spaces space;
    space.init(config);
    /////

    auto* dispatcher = space.dispatcher();

    auto session = duck_charmer::session_id_t();
    std::cerr << "query: " << args[0] << std::endl;
    auto res = dispatcher->execute_sql(session, args[0]);

    if (res.is_success()){
        show(res);
    }

    std::cerr << "error"<< std::endl;

    return 0;
}