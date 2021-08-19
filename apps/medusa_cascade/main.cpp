#include "database.hpp"
#include "dispatcher.hpp"
#include "collection.hpp"

#include <components/log/log.hpp>
#include <goblin-engineer/core.hpp>
#include <network_service/network_service.hpp>

int main(int argc, char* argv[]) {
    std::string log_dir("/tmp/docker_logs/");
    auto log = initialization_logger("medusa_cascade", log_dir);
    log.set_level(log_t::level::trace);

    auto manager_database = goblin_engineer::make_manager_service<kv::manager_database_t>(log, 1, 1000);
    auto database = goblin_engineer::make_manager_service<kv::database_t>(manager_database, log, 1, 1000);
    auto collection = goblin_engineer::make_service<kv::collection_t>(database, log);

    //auto manager_dispatcher = goblin_engineer::make_manager_service<kv::manager_dispatcher_t>(log, 1, 1000);
    //auto dispatcher = goblin_engineer::make_service<kv::dispatcher_t>(manager_dispatcher, log);

    net::io_context context;
    auto const address = net::ip::make_address("127.0.0.1");
    auto network_service = goblin_engineer::make_manager_service<network_service_t>(
        network_service_routes::name,
        context,
        tcp::endpoint{address, 9999},
        network_service_t::clients_t{},
        2,
        1000,
        log);

    auto dispatcher = goblin_engineer::make_service<kv::dispatcher_t>(network_service, log);


    goblin_engineer::link(dispatcher,collection);
    ///goblin_engineer::link(*network_service,dispatcher);

    auto sigint_set = std::make_shared<boost::asio::signal_set>(context, SIGINT, SIGTERM);
    sigint_set->async_wait(
        [&context, sigint_set](const boost::system::error_code& /*err*/, int /*num*/) {
            ZoneScoped;
            if (context.stopped()) {
                context.stop();
            }
            sigint_set->cancel();
        });

    context.run();
    return 0;
}
