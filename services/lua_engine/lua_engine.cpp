#include <rocketjoe/services/lua_engine/lua_engine.hpp>

#include <map>

#include <boost/utility/string_view.hpp>
#include <boost/filesystem.hpp>

#include <goblin-engineer/metadata.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <goblin-engineer/context.hpp>

#include <actor-zeta/actor/actor_address.hpp>

#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

namespace rocketjoe { namespace services {

            lua_vm::lua_vm(goblin_engineer::dynamic_config &configuration,
                                   goblin_engineer::abstract_environment *env) :
                    abstract_service(env, "lua_engine") {

                add_handler(
                        "dispatcher",
                        [this](actor_zeta::actor::context &, http::query_context &t) -> void {
                            pimpl->push_job(std::move(t));
                        }
                );


                add_handler(
                        "write",
                        [this](actor_zeta::actor::context &ctx) -> void {
                            ctx->addresses("http")->send(std::move(ctx.message()));
                        }
                );

                pimpl = std::make_unique<lua_engine::lua_context>(configuration, this->address());
            }

            void lua_vm::startup(goblin_engineer::context_t *) {
                pimpl->run();
            }

            void lua_vm::shutdown() {

            }

}}