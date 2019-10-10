#include <rocketjoe/services/python_engine/python_engine.hpp>

#include <iostream>

#include <boost/filesystem.hpp>

#include <goblin-engineer.hpp>
#include <actor-zeta/core.hpp>

#include <rocketjoe/services/python_engine/python_sandbox.hpp>

namespace rocketjoe { namespace services {

    python_vm::python_vm(network::server*ptr,goblin_engineer::dynamic_config &configuration)
        : abstract_service(ptr, "python_engine")
        {

        add_handler(
                "dispatcher",
                [this](actor_zeta::actor::context &,::rocketjoe::network::query_context&t) -> void {
                   std::cerr << "Warning" << std::endl;
                }
        );

        add_handler(
                "write",
                [this](actor_zeta::actor::context &ctx) -> void {
                    actor_zeta::send(ctx->addresses("http"),std::move(ctx.message()));
                }
        );

        pimpl = std::make_unique<python_engine::python_context>(configuration, this->address());

        pimpl->run();
    }
}}
