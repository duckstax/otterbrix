#include <cxxopts.hpp>

#include <boost/filesystem.hpp>

#include "configuration.hpp"

#include "init_service.hpp"

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


int main(int argc, char **argv) {

#ifdef __APPLE__

#else
    ::signal(SIGSEGV,&signal_sigsegv);

    std::set_terminate(terminate_handler);
#endif

    cxxopts::Options options("defaults", "");

    std::vector<std::string> positional;

    options.add_options()
            ("help", "Print help")
            ("data-dir", "data-dir",cxxopts::value<std::string>()->default_value("."))
            ("positional", "Positional parameters",cxxopts::value<std::vector<std::string>>(positional))
            ;


    std::vector<std::string> pos_names = {"positional"};

    options.parse_positional(pos_names.begin(), pos_names.end());

    auto result = options.parse(argc, argv);

    logo();

    /// --help option
    if (result.count("help")) {

        std::cout << options.help({}) << std::endl;

       return 0;
    }

    goblin_engineer::configuration config;

    load_or_generate_config(result,config);

    std::string path_app;

    if ( !positional.empty() ) {

        path_app = positional[0];

    } else if( config.dynamic_configuration.as_object().find("app") != config.dynamic_configuration.as_object().end() ) {

        path_app = config.dynamic_configuration.as_object()["app"].as_string();

    } else {

        std::cerr << "Not APP" << std::endl;

        return 1;
    }

    boost::filesystem::path path_app_ = boost::filesystem::absolute(path_app);

    if( !boost::filesystem::exists(path_app_) ){

        std::cerr << "Not Path APP" << std::endl;

        return 1;
    }

    config.dynamic_configuration.as_object()["app"] = path_app_.string();

    goblin_engineer::dynamic_environment env(std::move(config));

    init_service(env);

    env.initialize();

    env.startup();

    return 0;
}
