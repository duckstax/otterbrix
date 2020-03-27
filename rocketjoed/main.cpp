#include <cxxopts.hpp>

#include <cstdlib>
#include <exception>
#include <locale>

#include <boost/filesystem.hpp>
#include <boost/locale/generator.hpp>

#include <rocketjoe/log/log.hpp>
#include <rocketjoe/process_pool/process_pool.hpp>

#include "configuration.hpp"

#include "init_service.hpp"

#ifdef __APPLE__

#else

#include <csignal>
#include <boost/stacktrace.hpp>

void my_signal_handler(int signum) {
    ::signal(signum, SIG_DFL);
    std::cerr << "signal called:"
              << std::endl
              << boost::stacktrace::stacktrace()
              << std::endl;
    ::raise(SIGABRT);
}

void setup_handlers() {
    ::signal(SIGSEGV, &my_signal_handler);
    ::signal(SIGABRT, &my_signal_handler);
}

static auto original_terminate_handler{std::get_terminate()};

void terminate_handler() {
    std::cerr << "terminate called:"
              << std::endl
              << boost::stacktrace::stacktrace()
              << std::endl;
    original_terminate_handler();
    std::abort();
}

#endif

constexpr const static bool worker = false;

constexpr const static bool master = true;


int main(int argc, char *argv[]) {

#ifdef __APPLE__

#else
    setup_handlers();

    std::set_terminate(terminate_handler);
#endif

    auto log = rocketjoe::initialization_logger();

    std::locale::global(boost::locale::generator{}(""));

    cxxopts::Options options("defaults", "");

    std::vector<std::string> positional;

    options.add_options()
            ("help", "Print help")
            ("worker_mode", "Worker Process Mode",cxxopts::value<bool>())
            ("max_worker","Max worker",cxxopts::value<std::size_t >())
            ("jupyter_mode", "Jupyter kernel mode",cxxopts::value<bool>())
            ("data-dir", "data-dir", cxxopts::value<std::string>()->default_value("."))
            ("positional", "Positional parameters", cxxopts::value<std::vector<std::string>>(positional));

    std::vector<std::string> pos_names = {"positional"};

    options.parse_positional(pos_names.begin(), pos_names.end());

    options.allow_unrecognised_options();

    std::vector<std::string> all_args(argv, argv + argc);

    auto result = options.parse(argc, argv);

    log.info(logo());

    /// --help option
    if (result.count("help")) {

        std::cout << options.help({}) << std::endl;

        return 0;
    }

    goblin_engineer::dynamic_config config;

    load_or_generate_config(result, config);

    config.as_object()["master"] = master;

    if (result.count("worker_mode")) {

        log.info("Worker Mode");

        config.as_object()["master"] = worker;
    }

    if (result.count("jupyter_mode")) {
        log.info("Jupyter Mode");
    }

    rocketjoe::process_pool_t process_pool(all_args[0],{"--worker_mode=true"},log);

    config.as_object()["args"] = all_args;

    goblin_engineer::dynamic_config config_tmp = config;

    goblin_engineer::root_manager env(std::move(config));

    init_service(env, config_tmp);

    env.initialize();

    if(result.count("max_worker")) {
      process_pool.add_worker_process(result["max_worker"].as<std::size_t>()); /// todo hack
    }
    env.startup();

    return 0;
}
