#include <cstdlib>
#include <exception>
#include <locale>

#include <boost/filesystem.hpp>
#include <boost/locale/generator.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <rocketjoe/configuration/configuration.hpp>
#include <rocketjoe/log/log.hpp>
#include <rocketjoe/process_pool/process_pool.hpp>

#include "configuration.hpp"

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

    auto log = rocketjoe::initialization_logger();

    std::locale::global(boost::locale::generator{}(""));

    std::vector<std::string> all_args(argv, argv + argc);

    po::options_description command_line_description("Allowed options");
    // clang-format off
    command_line_description.add_options()
        ("help", "Print help")
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

    rocketjoe::configuration cfg_;
    cfg_.port_http_ = command_line["port_http"].as<unsigned short int>();
    int count_python_file = 0;

    auto it = std::find_if(
        all_args.begin(),
        all_args.end(),
        [&count_python_file](const std::string& value) {
            boost::filesystem::path p(value);
            if (!p.empty() && p.extension() == ".py") {
                count_python_file++;
                return true;
            }
            return false;
        });

    if (count_python_file == 1) {
        log.info("script mode");
        cfg_.python_configuration_.script_path_ = *it;
        cfg_.python_configuration_.mode_ = rocketjoe::sandbox_mode_t::script;

        if (command_line.count("jupyter_engine") && command_line.count("jupyter_connection")) {
            log.info("script mode + ipyparalle");
            cfg_.python_configuration_.jupyter_connection_path_ = command_line["jupyter_connection"].as<boost::filesystem::path>();
            cfg_.python_configuration_.mode_ = rocketjoe::sandbox_mode_t::jupyter_engine;
        } else {
            log.error("the jupyter_connection command line parameter is undefined");
            return 1;
        }
    }

    if (command_line.count("jupyter_kernel")) {
        log.info("jupyter kernel mode");
        if (command_line.count("jupyter_connection")) {
            cfg_.python_configuration_.jupyter_connection_path_ = command_line["jupyter_connection"].as<boost::filesystem::path>();
        } else {
            log.error("the jupyter_connection command line parameter is undefined");
            return 1;
        }

        cfg_.python_configuration_.mode_ = rocketjoe::sandbox_mode_t::jupyter_kernel;
    }

    if (command_line.count("jupyter_engine") && command_line.count("worker_number")) {
        log.info("jupyter engine mode");
        if (command_line.count("jupyter_connection")) {
            cfg_.python_configuration_.jupyter_connection_path_ = command_line["jupyter_connection"].as<boost::filesystem::path>();
        } else {
            log.error("the jupyter_connection command line parameter is undefined");
            return 1;
        }
        cfg_.python_configuration_.mode_ = rocketjoe::sandbox_mode_t::jupyter_engine;
    }

    cfg_.operating_mode_ = rocketjoe::operating_mode::master;

    if (command_line.count("worker_number")) {
        log.info("Worker Mode");
        cfg_.operating_mode_ = rocketjoe::operating_mode::worker;
    }

    goblin_engineer::components::root_manager env(1, 1000);
    rocketjoe::process_pool_t process_pool(all_args[0], {"--worker_number 0"}, log);
    init_service(env, cfg_, log);

    if (command_line.count("worker_number")) {
        process_pool.add_worker_process(command_line["worker_number"].as<std::size_t>()); /// todo hack
    }

    boost::asio::signal_set sigint_set(env.loop(), SIGINT, SIGTERM);
    sigint_set.async_wait(
        [&sigint_set](const boost::system::error_code& /*err*/, int /*num*/) {
            sigint_set.cancel();
        });

    env.startup();

    log.info("Shutdown RocketJoe");
    return 0;
}