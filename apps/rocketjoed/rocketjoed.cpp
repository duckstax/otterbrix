#include <iostream>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>

#include <goblin-engineer/dynamic_environment.hpp>

#include <yaml-cpp/yaml.h>

#include "init_service.hpp"

constexpr const char *config_name_file = "config.yaml";

constexpr const char *data_name_file = "rocketjoe";

constexpr const char *plugins_name_file = "plugins";

constexpr const char *section_default = "default";


#ifdef __APPLE__

#else

#include <boost/stacktrace.hpp>

void terminate_handler() {
    std::cerr << "terminate called:"
              << std::endl
              << boost::stacktrace::stacktrace()
              << std::endl;
}

void signal_sigsegv(int signum){
    boost::stacktrace::stacktrace bt ;
    if(bt){
        std::cerr << "Signal"
                  << signum
                  << " , backtrace:"
                  << std::endl
                  << boost::stacktrace::stacktrace()
                  << std::endl;
    }
    std::abort();
}

#endif


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
            output = input.as<std::string>();
            break;
        }

    } /// switch

}

auto logo() {

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

auto load_config( boost::program_options::variables_map& args_, goblin_engineer::configuration & config ) {

    std::cout << args_["data-dir"].as<std::string>() <<std::endl;
    config.data_dir = args_["data-dir"].as<std::string>();

    boost::filesystem::path config_path = config.data_dir / config_name_file;

    config.dynamic_configuration.as_object().emplace("config-path",(config.data_dir/data_name_file).string());

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

auto generate_yaml_config( YAML::Node& config_ ) {

    config_["address"] = "127.0.0.1";
    config_["ws-port"] = 9999;
    config_["http-port"] = 9998;
    config_["mongo-uri"] = "mongodb://localhost:27017/rocketjoe";
    config_["env-lua"]["http"] = "lua/http.lua";

}

auto generate_config( goblin_engineer::configuration & config ) {

    config.data_dir = boost::filesystem::current_path();

    auto data_path = config.data_dir / data_name_file;

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

    goblin_engineer::dynamic_config json_config;
    YAML::Node config_ = YAML::LoadFile(config_path.string());
    convector(config_,config.dynamic_configuration);

}

int main(int argc, char **argv) {
    
#ifdef __APPLE__

#else
    ::signal(SIGSEGV,&signal_sigsegv);

    std::set_terminate(terminate_handler);
#endif

    boost::program_options::options_description app_options_;

    app_options_.add_options()
            ("help", "Print help messages")
            ("data-dir", "data-dir")
            ("app", boost::program_options::value<std::string>(), "app111");


    boost::program_options::variables_map args_;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, app_options_), args_);
    boost::program_options::notify(args_);

    /** --help option
    */
    if (args_.count("help")) {
        std::cout << "Rocket Joe Command Line Parameter" << std::endl << app_options_ << std::endl;
        return 0;
    }

    goblin_engineer::configuration config;

    if (args_.count("data-dir")) {

        load_config(args_,config);

    } else {

        generate_config(config);

    }

    if ( args_.count("app") ) {

        std::cerr << "app :"  << args_["app"].as<std::string>() << std::endl;

        if( boost::filesystem::exists(args_["app"].as<std::string>())){

            config.dynamic_configuration.as_object()["app"] = args_["app"].as<std::string>();

        } else {
            std::cout << "Not App" << std::endl;
            return 1;
        }

    } else {

        std::cout << "Not App" << std::endl;
        return 1;
    }

    logo();

    goblin_engineer::dynamic_environment env(std::move(config));

    init_service(env);

    env.initialize();

    env.startup();

    return 0;
}