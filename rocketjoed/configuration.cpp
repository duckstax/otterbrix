#include <string>
#include <utility>

#include "configuration.hpp"

#include <yaml-cpp/yaml.h>

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>

#include <nlohmann/json.hpp>

constexpr const char* config_name_file = "config.yaml";

/// constexpr const char *data_name_file = "rocketjoe";

/// constexpr const char *plugins_name_file = "plugins";

/// constexpr const char *section_default = "default";

void convector(YAML::Node& input, nlohmann::json& output) {
    switch (input.Type()) {
        case YAML::NodeType::Map: {
            for (YAML::const_iterator it1 = input.begin(); it1 != input.end(); ++it1) {
                std::string key = it1->first.as<std::string>();
                YAML::Node value = it1->second;
                nlohmann::json tmp;
                convector(value, tmp);
                output = nlohmann::json::object();
                output.emplace(key, tmp);
            }

            break;
        }

        case YAML::NodeType::Sequence: {
            for (YAML::const_iterator it1 = input.begin(); it1 != input.end(); ++it1) {
                YAML::Node value = (*it1);
                nlohmann::json tmp;
                convector(value, tmp);
                output = nlohmann::json::array();
                output.emplace_back(tmp);
            }

            break;
        }

        case YAML::NodeType::Scalar: {
            output = nlohmann::json::string_t();
            output  = input.as<std::string>();
            break;
        }

        case YAML::NodeType::Undefined: {
            break;
        }
        case YAML::NodeType::Null: {
            break;
        }
    } /// switch
}

auto logo() -> const char* {
    /// clang-format off
    constexpr const char* logo_ = R"__(
-------------------------------------------------
______           _        _     ___
| ___ \         | |      | |   |_  |
| |_/ /___   ___| | _____| |_    | | ___   ___
|    // _ \ / __| |/ / _ \ __|   | |/ _ \ / _ \
| |\ \ (_) | (__|   <  __/ |_/\__/ / (_) |  __/
\_| \_\___/ \___|_|\_\___|\__\____/ \___/ \___|
-------------------------------------------------
    )__";
    /// clang-format on
    return logo_;
}

void load_config(cxxopts::ParseResult& args_, nlohmann::json& config) {
    auto data_dir = boost::filesystem::path(args_["data-dir"].as<std::string>());

    boost::filesystem::path config_path = data_dir / config_name_file;

    /// config.as_object().emplace("config-path",(config.data_dir).string());

    nlohmann::json json_config = nlohmann::json::object();
    YAML::Node config_ = YAML::LoadFile(config_path.string());
    convector(config_, config);

    if (args_.count("plugins")) {
        ///        auto &plugins = args_["plugins"].as<std::vector<std::string>>();
        ///        for (auto &&i:plugins) {
        ///            config.plugins.emplace(std::move(i));
        ///        }
    }
}

void generate_yaml_config(YAML::Node& config_) {
    config_["address"] = "127.0.0.1";
    config_["websocket-port"] = 9999;
    config_["http-port"] = 9998;
    config_["ipc_message_queue_id"] = boost::uuids::to_string(boost::uuids::random_generator()());
}

void generate_config(nlohmann::json& config, boost::filesystem::path data_dir_) {
    ///config.data_dir = std::move(data_dir_);

    auto data_path = data_dir_;

    ///config.as_object().emplace("config-path",data_path.string());

    ///if (!boost::filesystem::exists(data_path)) {
    ///    boost::filesystem::create_directories(data_path);
    //}

    ///if (!boost::filesystem::exists(data_path / plugins_name_file)) {
    ///    boost::filesystem::create_directories(data_path / plugins_name_file);
    ///    config.plugins_dir = data_path / plugins_name_file;
    //}

    boost::filesystem::path config_path = data_path / config_name_file;

    if (!boost::filesystem::exists(config_path.string())) {
        YAML::Node config_;
        generate_yaml_config(config_);
        std::ofstream outfile(config_path.string());
        outfile << config_;
        outfile.flush();
        outfile.close();
    }

    YAML::Node config_ = YAML::LoadFile(config_path.string());
    convector(config_, config);
}

constexpr const static bool master = true;

void load_or_generate_config(
    cxxopts::ParseResult& result,
    nlohmann::json& cfg) {
    if (cfg["master"].get<bool>() == master) {
        if (result.count("data-dir")) {
            boost::filesystem::path data_dir(result["data-dir"].as<std::string>());
            data_dir = boost::filesystem::absolute(data_dir);

            if (!boost::filesystem::exists(data_dir)) {
                generate_config(cfg, data_dir);

            } else {
                load_config(result, cfg);
            }

        } else {
            generate_config(cfg, boost::filesystem::current_path());
        }
    }
}
