#include <csignal>

#include <iostream>

#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <boost/stacktrace.hpp>

#include "log/log.hpp"
#include <goblin-engineer/core.hpp>

#include "network_service/network_service.hpp"

using namespace std::chrono_literals;

void my_signal_handler(int signum) {
    ::signal(signum, SIG_DFL);
    std::cerr << "signal called:" << std::endl
              << boost::stacktrace::stacktrace() << std::endl;
    ::raise(SIGABRT);
}

void setup_handlers() {
    ::signal(SIGSEGV, &my_signal_handler);
    ::signal(SIGABRT, &my_signal_handler);
}

static auto original_terminate_handler{std::get_terminate()};

void terminate_handler() {
    std::cerr << "terminate called:" << std::endl
              << boost::stacktrace::stacktrace() << std::endl;
    original_terminate_handler();
    std::abort();
}

class dispatcher_t : public goblin_engineer::abstract_service {
public:
    explicit dispatcher_t(actor_zeta::intrusive_ptr<network_service_t> ptr)
        : goblin_engineer::abstract_service(ptr, "dispatcher") {
        add_handler("http_dispatch", &dispatcher_t::http_dispatch);
        add_handler("ws_dispatch", &dispatcher_t::ws_dispatch);
        log_ = get_logger();
    }

    void http_dispatch(std::uintptr_t id, request_t& request) {
        auto request_tmp = std::move(request);
        log_.info("http_dispatch");
        log_.info(request_tmp.body().c_str());
        response_t response;
        response.result(200);
        actor_zeta::send(addresses("network_service"), self(), "http_write", id, std::move(response));
    }

    void ws_dispatch(std::uintptr_t id, std::string& request, size_t size) {
        auto request_tmp = std::move(request);
        log_.info("http_dispatch");
        log_.info(request_tmp);

        actor_zeta::send(addresses("network_service"), self(), "ws_write", id, std::move(request_tmp));
    }

private:
    log_t log_;
};

int main(int argc, char** argv) {
    setup_handlers();
    std::set_terminate(terminate_handler);
    auto log = initialization_logger("demo_horus", ".");
    log.set_level(log_t::level::info);
    auto const address = net::ip::make_address("0.0.0.0");
    unsigned short const http_port = 8000;

    boost::asio::io_context context;
    auto ns =
        goblin_engineer::make_manager_service<network_service_t>(
            network_service_routes::name,
            context,
            tcp::endpoint{address, http_port},
            network_service_t::clients_t{},
            1, 1000, log);
    auto dispatcher = goblin_engineer::make_manager_service<dispatcher_t>(ns);
    auto address_i = dispatcher->address();
    goblin_engineer::link(*ns, address_i);
    ///std::thread t([&]() {
    ///    context.run();
    ///});

    std::string host("0.0.0.0");
    std::string port("5000");
    std::string target("/v1/detection/jsonrpc");

    /*
    while (true) {
        request_t request;
        make_http_client_request(host, target, http::verb::post, request);
        //@TODO//actor_zeta::send(ns->address(), address_i, "http_client_write", host, port, std::move(request));
        std::this_thread::sleep_for(1s);
    }

    t.join();
     */
    context.run();
    return 0;
}
