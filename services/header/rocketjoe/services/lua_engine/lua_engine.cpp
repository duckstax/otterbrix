#include <rocketjoe/services/lua_engine/lua_engine.hpp>

#include <map>

#include <boost/utility/string_view.hpp>
#include <boost/filesystem.hpp>

#include <goblin-engineer.hpp>

#include <actor-zeta/core.hpp>

#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

namespace rocketjoe { namespace services {

            lua_vm::lua_vm(network::server* ptr,goblin_engineer::dynamic_config&configuration)
                : abstract_service(ptr, "lua_engine")
                , pimpl(std::make_unique<lua_engine::lua_context>(configuration, this->address())) {

                add_handler(
                        "dispatcher",
                        [this](actor_zeta::actor::context &, network::query_context &t) -> void {
                            pimpl->push_job(std::move(t));
                        }
                );

                add_handler(
                        "write",
                        [this](actor_zeta::actor::context &ctx) -> void {
                            actor_zeta::send(ctx->addresses("http"),std::move(ctx.message()));
                        }
                );

                pimpl->run();
            }

}}