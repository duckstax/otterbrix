#include <cstdlib>
#include <exception>
#include <locale>

#include <boost/filesystem.hpp>
#include <boost/locale/generator.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/process_pool/process_pool.hpp>

#include <zmq.hpp>

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

int main(int argc, char* argv[]) {
#ifdef __APPLE__

#else
    setup_handlers();

    std::set_terminate(terminate_handler);
#endif

    auto log = components::initialization_logger();

    rocketjoe::configuration cfg_;

    std::vector<std::string> all_args(argv, argv + argc);

    goblin_engineer::components::root_manager env(1, 1000);

    auto ctx = std::make_unique<zmq::context_t>();

    init_service(env, cfg_, log,*ctx);

    boost::asio::signal_set sigint_set(env.loop(), SIGINT, SIGTERM);
    sigint_set.async_wait(
        [&sigint_set](const boost::system::error_code& /*err*/, int /*num*/) {
            sigint_set.cancel();
        });

    env.startup();
    ctx->close();
    log.info("Shutdown RocketJoe");
    return 0;
}
