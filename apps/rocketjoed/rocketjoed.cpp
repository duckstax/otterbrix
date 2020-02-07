#include <cxxopts.hpp>

#include <boost/filesystem.hpp>

#include "configuration.hpp"

#include "init_service.hpp"

#ifdef __APPLE__

#else

#include <signal.h>
#include <boost/stacktrace.hpp>

void my_signal_handler(int signum) {
    ::signal(signum, SIG_DFL);
    boost::stacktrace::safe_dump_to("./backtrace.dump");
    ::raise(SIGABRT);
}

void setup_handlers() {
    ::signal(SIGSEGV, &my_signal_handler);
    ::signal(SIGABRT, &my_signal_handler);
}

void terminate_handler() {
    std::cerr << "terminate called:"
              << std::endl
              << boost::stacktrace::stacktrace()
              << std::endl;
}

#endif


int main(int argc, char *argv[]) {

#ifdef __APPLE__

#else
    setup_handlers();

    std::set_terminate(terminate_handler);
#endif

    cxxopts::Options options("defaults", "");

    std::vector<std::string> positional;

    options.add_options()
            ("help", "Print help")
            ("worker_mode", "Worker Process Mode")
            ("jupyter_mode", "Jupyter kernel mode")
            ("data-dir", "data-dir", cxxopts::value<std::string>()->default_value("."))
            ("positional", "Positional parameters", cxxopts::value<std::vector<std::string>>(positional));

    std::vector<std::string> pos_names = {"positional"};

    options.parse_positional(pos_names.begin(), pos_names.end());

    options.allow_unrecognised_options();

    std::vector<std::string> all_args(argv, argv + argc);

    auto result = options.parse(argc, argv);

    logo();

    /// --help option
    if (result.count("help")) {

        std::cout << options.help({}) << std::endl;

        return 0;
    }

    goblin_engineer::dynamic_config config;

    load_or_generate_config(result, config);

    config.as_object()["master"] = true;

    if (result.count("worker_mode")) {

        std::cerr << "Worker Mode" << std::endl;

        config.as_object()["master"] = false;
    }

    if (result.count("jupyter_mode")) {

        std::cerr << "Jupyter Mode" << std::endl;
    }

    config.as_object()["args"] = all_args;

    goblin_engineer::dynamic_config config_tmp = config;

    goblin_engineer::root_manager env(std::move(config));

    init_service(env, config_tmp);

    env.initialize();

    env.startup();

    return 0;
}