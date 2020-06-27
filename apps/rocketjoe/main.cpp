#include <cstdlib>
#include <exception>
#include <locale>

#include <boost/filesystem.hpp>
#include <boost/locale/generator.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "configuration.hpp"
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/process_pool/process_pool.hpp>
#include <components/python_sandbox/python_sandbox.hpp>

#include "init_service.hpp"

#ifdef __APPLE__

#else

#include <boost/stacktrace.hpp>
#include <csignal>

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

namespace po = boost::program_options;

int main(int argc, char* argv[]) {
#ifdef __APPLE__

#else
    setup_handlers();

    std::set_terminate(terminate_handler);
#endif

    auto log = components::initialization_logger();

    std::locale::global(boost::locale::generator{}(""));

    std::vector<std::string> all_args(argv, argv + argc);

    po::options_description command_line_description("Allowed options");
    // clang-format off
    command_line_description.add_options()
        ("help", "Print help")
        ("worker_mode", "Worker Mode")
        ("worker_number",po::value<unsigned short int>(), "Worker Process Mode and run  ")
        ("jupyter_kernel", "Jupyter kernel mode")
        ("jupyter_engine", "Ipyparalle mode")
        ("port_http",po::value<unsigned short int>()->default_value(9999), "Port Http")
        ("jupyter_connection", po::value<boost::filesystem::path>(),"path to jupyter connection file")
        ;
    // clang-format on
    po::variables_map command_line;

    po::store(
        po::command_line_parser(all_args)
            .options(command_line_description)
            .allow_unregistered() /// todo hack
            .run(),
        command_line);

    log.info(logo());
    log.info(fmt::format("Command  Args : {0}", fmt::join(all_args, " , ")));

    /// --help option
    if (command_line.count("help")) {
        std::cerr << command_line_description << std::endl;
        return 0;
    }

    components::configuration cfg_;

    if (command_line.count("port_http")) {
        cfg_.port_http_ = command_line["port_http"].as<unsigned short int>();
    } else {
        log.info("default port : 9999");
    }

    if (command_line.count("jupyter_kernel") && command_line.count("jupyter_engine")) {
        log.error("configuration conflict jupyter_kernel and jupyter_engine");
        return 1;
    }

    if (command_line.count("jupyter_kernel") && command_line.count("worker_number")) {
        log.error("configuration conflict jupyter_kernel and worker_number");
        return 1;
    }

    int number_of_python_files = 0;

    /// TODO: print non-existing files
    for (const auto& i : all_args) {
        boost::filesystem::path p(i);
        auto non_empty = boost::filesystem::exists(p);
        auto extension = p.extension();
        if ((non_empty && extension == ".py")) {
            if (number_of_python_files == 0) {
                cfg_.python_configuration_.script_path_ = i;
            }
            number_of_python_files++;
        }
    }

    bool exist_path_script_python_file = !cfg_.python_configuration_.script_path_.empty();

    if (number_of_python_files > 1) {
        log.error("More than one file to run!");
        log.error(fmt::format("Number of files: {0}", number_of_python_files));
        log.error("More than one file to run!");
        return 1;
    }

    if (exist_path_script_python_file && command_line.count("jupyter_kernel")) {
        log.error("configuration conflict jupyter_kernel and script mode");
        return 1;
    }

    if (exist_path_script_python_file && command_line.count("jupyter_engine")) {
        log.error("configuration conflict jupyter_engine and script mode");
        return 1;
    }

    if (exist_path_script_python_file && command_line.count("jupyter_connection")) {
        log.error("configuration conflict jupyter_* and script mode");
        return 1;
    }

    if (exist_path_script_python_file) {
        log.info("script mode");
        log.info(fmt::format("run: {0}", (cfg_.python_configuration_.script_path_.string())));
        cfg_.python_configuration_.mode_ = components::sandbox_mode_t::script;
    }

    if (command_line.count("jupyter_kernel")) {
        log.info("jupyter kernel mode");
        cfg_.python_configuration_.mode_ = components::sandbox_mode_t::jupyter_kernel;
    }

    if (command_line.count("jupyter_engine")) {
        log.info("jupyter engine mode");
        cfg_.python_configuration_.mode_ = components::sandbox_mode_t::jupyter_engine;
    }

    if (command_line.count("jupyter_connection")) {
        cfg_.python_configuration_.jupyter_connection_path_ = command_line["jupyter_connection"].as<boost::filesystem::path>();
    }

    cfg_.operating_mode_ = components::operating_mode::master;

    if (command_line.count("worker_mode")) {
        log.info("Worker Mode");
        cfg_.operating_mode_ = components::operating_mode::worker;
    }

    if (cfg_.python_configuration_.mode_ == components::sandbox_mode_t::none) {
        log.error("initialization error: mode is none");
        return 1;
    }

    goblin_engineer::components::root_manager env(1, 1000);
    components::process_pool_t process_pool(all_args[0], {"--worker_mode"}, log);
    init_service(env, cfg_, log);

    if (command_line.count("worker_number")) {
        process_pool.add_worker_process(command_line["worker_number"].as<std::size_t>()); /// todo hack
    }

    switch (cfg_.python_configuration_.mode_) {
        case components::sandbox_mode_t::none: {
            log.error("invalid state: none");
            return 1;
        }
        case components::sandbox_mode_t::script: {
            components::python_interpreter vm(cfg_.python_configuration_,log);
            vm.init();
            vm.run_script(all_args);
            break;
        }
        case components::sandbox_mode_t::jupyter_kernel:
        case components::sandbox_mode_t::jupyter_engine: {
            boost::asio::signal_set sigint_set(env.loop(), SIGINT, SIGTERM);
            sigint_set.async_wait(
                [&sigint_set](const boost::system::error_code& /*err*/, int /*num*/) {
                    sigint_set.cancel();
                });

            env.startup();
            break;
        }
        default:{
            log.error("invalid state");
            break;
        }
    }

    log.info("Shutdown RocketJoe");
    return 0;
}
