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
        /// TODO: add support joblib ("worker_mode", "Worker Mode")
        /// TODO: add support joblib ("worker_number",po::value<unsigned short int>(), "Worker Process Mode and run  ")
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

    cfg_.python_configuration_.mode_= components::sandbox_mode_t::script;
    cfg_.operating_mode_ = components::operating_mode::master;

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

    if (exist_path_script_python_file) {
        log.info("script mode");
        log.info(fmt::format("run: {0}", (cfg_.python_configuration_.script_path_.string())));
        cfg_.python_configuration_.mode_ = components::sandbox_mode_t::script;
    }

    if (command_line.count("worker_mode")) {
        log.info("Worker Mode");
        cfg_.operating_mode_ = components::operating_mode::worker;
    }

    goblin_engineer::components::root_manager env(1, 1000);

    /// TODO: add support joblib components::process_pool_t process_pool(all_args[0], {"--worker_mode"}, log);

    init_service(env, cfg_, log);

    /// TODO: add support joblib if (command_line.count("worker_number")) {
    /// process_pool.add_worker_process(command_line["worker_number"].as<std::size_t>()); /// todo hack
    /// }

    components::python_interpreter vm(cfg_.python_configuration_, log);

    vm.run_script(all_args);

    log.info("Shutdown RocketJoe");
    return 0;
}
