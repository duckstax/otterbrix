#include "init_service.hpp"

#include <goblin-engineer/components/http.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <iostream>

#include <services/storage/storage.hpp>
#include <services/zmq-hub/zmq_hub.hpp>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;
using namespace goblin_engineer::components;

using actor_zeta::link;

class controller_t final : public goblin_engineer::abstract_service {
public:
    template<class Manager>
    controller_t(actor_zeta::intrusive_ptr<Manager> ptr):abstract_service(ptr,"controller") {
        add_handler("controller",&controller_t::dispatcher);
    }

    void dispatcher() {

    }
};

void init_service(goblin_engineer::components::root_manager&  env, rocketjoe::configuration&cfg, components::log_t&log){

    auto zmq_context = std::make_unique<zmq::context_t>();

    auto zmq_hub = make_manager_service<services::zmq_hub_t>(env,log,std::move(zmq_context));
   ///services::make_lisaner_zmq_socket(*zmq_context,zmq_hub,services::make_url("","",9999), zmq::socket_type::router);
    auto storage = make_service<services::storage_hub>(zmq_hub);
    auto controller = make_service<controller_t>(zmq_hub);
    link(controller,storage);


    http::router router_;

    router_.http_get(
        R"(/my/super/url)", [](http::query_context& ctx) {
            std::cerr << ctx.response().body().c_str() << std::endl;
            ctx.response().body() = ctx.request().body();
            ctx.write();
        });

    auto http = make_manager_service<http::server>(env, cfg.port_http_);
    make_service<http::http_dispatcher>(http, router_);

}
