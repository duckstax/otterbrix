#include <iostream>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>

#include <goblin-engineer/dynamic_environment.hpp>

#include <rocketjoe/services/lua_engine/lua_engine.hpp>
#include <rocketjoe/services/ws_server/ws_server.hpp>
#include <rocketjoe/services/http_server/http_server.hpp>
#include <rocketjoe/services/database/database.hpp>
#include <rocketjoe/services/object_storage/object_storage.hpp>

#include <yaml-cpp/yaml.h>


constexpr const char *config_name_file = "config.yaml";

constexpr const char *data_name_file = "data";

constexpr const char *plugins_name_file = "plugins";

constexpr const char *section_default = "default";


void convector(YAML::Node &input, goblin_engineer::dynamic_config &output) {

    for (YAML::const_iterator it = input.begin(); it != input.end(); ++it) {
        std::string key = it->first.as<std::string>();
        YAML::Node value = it->second;
        switch (value.Type()) {

            case YAML::NodeType::Scalar: {
                output.as_object().emplace(key, value.as<std::string>());
                break;
            }
            case YAML::NodeType::Sequence: {
                break;
            }

            case YAML::NodeType::Map: {
                goblin_engineer::dynamic_config tmp;
                convector(value,tmp);
                output.as_object().emplace(key,tmp);
                break;
            }

            case YAML::NodeType::Undefined: {
                break;
            }

            case YAML::NodeType::Null: {
                break;
            }
        }
    }


}

int main(int argc, char **argv) {
    boost::program_options::variables_map args_;
    boost::program_options::options_description app_options_;

    app_options_.add_options()
            ("help", "Print help messages")
            ("data-dir", "data-dir")
            ("scripts", boost::program_options::value<std::vector<std::string>>()->multitoken(), "scripts")
            ("plugins", boost::program_options::value<std::vector<std::string>>()->multitoken(), "plugins");

    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, app_options_), args_);

    /** --help option
    */
    if (args_.count("help")) {
        std::cout << "Basic Command Line Parameter App" << std::endl << app_options_ << std::endl;
        return 0;
    }



    //boost::program_options::notify(args_);

    boost::filesystem::path data_dir;
    boost::filesystem::path config_path;
    boost::filesystem::path plugins_dir;


    if (args_.count("data-dir")) {

        data_dir = args_["data-dir"].as<std::string>();

        config_path = data_dir / config_name_file;

    } else {

        data_dir = boost::filesystem::current_path();

        auto data_path = data_dir / data_name_file;

        if (!boost::filesystem::exists(data_path)) {
            boost::filesystem::create_directories(data_path);
        }

        if (!boost::filesystem::exists(data_path / plugins_name_file)) {
            boost::filesystem::create_directories(data_path / plugins_name_file);
            plugins_dir = data_path / plugins_name_file;
        }

        config_path = data_path / config_name_file;

        if (!boost::filesystem::exists(config_path.string())) {
            std::ofstream outfile(config_path.string());
            outfile.close();
        }

    }

    goblin_engineer::configuration config;

    if (args_.count("plugins")) {
        auto &plugins = args_["plugins"].as<std::vector<std::string>>();
        for (auto &&i:plugins) {
            config.plugins.emplace(std::move(i));
        }
    }

    goblin_engineer::dynamic_config json_config;
    YAML::Node config_ = YAML::LoadFile(config_path.string());
    convector(config_,json_config);

    config.data_dir = data_dir.string();
    config.dynamic_configuration = json_config;
    config.plugins_dir = plugins_dir.string();


    goblin_engineer::dynamic_environment env(std::move(config));

    /// rewrite config
    auto &lua = env.add_service<rocketjoe::services::lua_engine::lua_engine>();
    /// rewrite config

    auto& database = env.add_service<rocketjoe::services::database::database>();

    auto& router = env.add_service<rocketjoe::services::object_storage::object_storage>();

    auto &http = env.add_service<rocketjoe::services::http_server::http_server>();

    ///http <-> router
    http->join(router);
    router->join(http);
    ///http <-> router

    ///lua <-> http
    lua->join(http);
    http->join(lua);
    ///lua <-> http

    ///router <-> database
    router->join(database);
    database->join(router);
    ///router <-> database

    /// lua <-> database
    lua->join(database);
    database->join(lua);
    /// lua <-> database

    env.initialize();

    env.startup();

    return 0;
}