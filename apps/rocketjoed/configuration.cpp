#include <utility>

#include <iostream>

#include "configuration.hpp"

#include <yaml-cpp/yaml.h>

constexpr const char *config_name_file = "config.yaml";

constexpr const char *data_name_file = "rocketjoe";

constexpr const char *plugins_name_file = "plugins";

constexpr const char *section_default = "default";

void convector(YAML::Node &input, goblin_engineer::dynamic_config &output) {

    switch (input.Type()) {

        case YAML::NodeType::Map: {

            for (YAML::const_iterator it1 = input.begin(); it1 != input.end(); ++it1) {

                std::string key = it1->first.as<std::string>();
                YAML::Node value = it1->second;
                goblin_engineer::dynamic_config tmp;
                convector(value, tmp);
                output.as_object().emplace(key, tmp);

            }

            break;
        }

        case YAML::NodeType::Sequence: {

            for (YAML::const_iterator it1 = input.begin(); it1 != input.end(); ++it1) {

                YAML::Node value = (*it1);
                goblin_engineer::dynamic_config tmp;
                convector(value, tmp);
                output.as_array().emplace_back(tmp);

            }

            break;
        }


        case YAML::NodeType::Scalar: {
            output.as_string() = input.as<std::string>();
            break;
        }

        case YAML::NodeType::Undefined:{break;}
        case YAML::NodeType::Null:{break;}
    } /// switch

}

void logo() {

    std::cout << std::endl;

    std::cout << "-------------------------------------------------";

    std::cout << "\n"
                 "\n"
                 "______           _        _       ___            \n"
                 "| ___ \\         | |      | |     |_  |           \n"
                 "| |_/ /___   ___| | _____| |_      | | ___   ___ \n"
                 "|    // _ \\ / __| |/ / _ \\ __|     | |/ _ \\ / _ \\\n"
                 "| |\\ \\ (_) | (__|   <  __/ |_  /\\__/ / (_) |  __/\n"
                 "\\_| \\_\\___/ \\___|_|\\_\\___|\\__| \\____/ \\___/ \\___|\n"
                 "                                                 \n"
                 "                                                 \n"
                 "";


    std::cout << "-------------------------------------------------";

    std::cout << std::endl;
    std::cout << std::endl;

    std::cout.flush();

}

void load_config(cxxopts::ParseResult &args_, goblin_engineer::configuration &config) {

    config.data_dir = args_["data-dir"].as<std::string>();

    boost::filesystem::path config_path = config.data_dir / config_name_file;

    config.dynamic_configuration.as_object().emplace("config-path",(config.data_dir).string());

    goblin_engineer::dynamic_config json_config;
    YAML::Node config_ = YAML::LoadFile(config_path.string());
    convector(config_,config.dynamic_configuration);

    if (args_.count("plugins")) {
        auto &plugins = args_["plugins"].as<std::vector<std::string>>();
        for (auto &&i:plugins) {
            config.plugins.emplace(std::move(i));
        }
    }

}

void generate_yaml_config(YAML::Node &config_) {

    config_["address"] = "127.0.0.1";
    config_["ws-port"] = 9999;
    config_["http-port"] = 9998;
    config_["mongo-uri"] = "mongodb://localhost:27017/rocketjoe";
    config_["env-lua"]["http"] = "lua/http.lua";

}

void generate_config(goblin_engineer::configuration &config,boost::filesystem::path data_dir_) {

    config.data_dir = std::move(data_dir_);

    auto data_path = config.data_dir ;

    config.dynamic_configuration.as_object().emplace("config-path",data_path.string());

    if (!boost::filesystem::exists(data_path)) {
        boost::filesystem::create_directories(data_path);
    }

    if (!boost::filesystem::exists(data_path / plugins_name_file)) {
        boost::filesystem::create_directories(data_path / plugins_name_file);
        config.plugins_dir = data_path / plugins_name_file;
    }

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
    convector(config_,config.dynamic_configuration);

}

void load_or_generate_config(cxxopts::ParseResult& result, goblin_engineer::configuration& configuration) {

    if (result.count("data-dir")) {

        boost::filesystem::path data_dir(result["data-dir"].as<std::string>());
        data_dir = boost::filesystem::absolute(data_dir);

        if(!boost::filesystem::exists(data_dir)){

            generate_config(configuration,data_dir);

        } else {

            load_config(result,configuration);
        }

    } else {

        generate_config(configuration,boost::filesystem::current_path());

    }

}
