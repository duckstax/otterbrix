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

    cxxopts::Options options("defaults", "has defaults");

    std::vector<std::string> positional;

    options.add_options()
            ("help", "Print help")
            ("generate", "Input", cxxopts::value<std::string>()->default_value("."))
            ("data-dir", "Print help",cxxopts::value<std::string>()->default_value("."))
            ("positional", "Positional parameters",cxxopts::value<std::vector<std::string>>(positional))
            ;


    std::vector<std::string> pos_names = {"positional"};

    options.parse_positional(pos_names.begin(), pos_names.end());

    auto result = options.parse(argc, argv);




    /** --help option
    */
    if (result.count("help")) {
       ///std::cout << "Rocket Joe Command Line Parameter" << std::endl << options.show_positional_help() << std::endl;
        return 0;
    }

    goblin_engineer::configuration config;


    if (result.count("generate")){

        generate_config(config);
        return 0;
    }

    if (result.count("data-dir")) {

        load_config(result,config);

    } else {

        generate_config(config);

    }

    if ( result.count("app") ) {

        std::cerr << "app :"  << result["app"].as<std::string>() << std::endl;

        if( boost::filesystem::exists(result["app"].as<std::string>())){

            config.dynamic_configuration.as_object()["app"] = result["app"].as<std::string>();

        } else {
            std::cout << "Not App" << std::endl;
            return 1;
        }

    } else {

        std::cout << "Not App" << std::endl;

        return 1;
    }

    goblin_engineer::dynamic_environment env(std::move(config));

    init_service(env);

    env.initialize();

    env.startup();

    return 0;
}