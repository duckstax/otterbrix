#include <rocketjoe/services/python_engine/python_engine.hpp>

#include <map>

#include <boost/utility/string_view.hpp>
#include <boost/filesystem.hpp>

#include <goblin-engineer/metadata.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <goblin-engineer/context.hpp>

#include <actor-zeta/actor/actor_address.hpp>

#include <rocketjoe/services/python_engine/python_sandbox.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

    python_engine::python_engine(goblin_engineer::dynamic_config& configuration,goblin_engineer::abstract_environment * env):
            abstract_service(env, "python_engine") {

        attach(
                actor_zeta::behavior::make_handler(
                        "dispatcher",
                        [this](actor_zeta::behavior::context &,http::query_context&t) -> void {
                            pimpl->push_job(std::move(t));
                        }
                )
        );


        attach(
                actor_zeta::behavior::make_handler(
                        "write",
                        [this](actor_zeta::behavior::context &ctx) -> void {
                            ctx->addresses("http")->send(std::move(ctx.message()));
                        }
                )
        );

        pimpl = std::make_unique<python_context>(configuration, this->address());
    }

    void python_engine::startup(goblin_engineer::context_t *) {
        pimpl->run();
    }

    void python_engine::shutdown() {

    }

}}}
