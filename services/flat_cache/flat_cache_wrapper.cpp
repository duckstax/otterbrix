#include <rocketjoe/services/flat_cache/flat_cache_wrapper.hpp>
#include <rocketjoe/services/flat_cache/flat_cache_impl.hpp>
#include <rocketjoe/api/cache_commands.hpp>

#include <goblin-engineer/metadata.hpp>

namespace rocketjoe { namespace services { namespace flat_cache {

            class flat_cache_wrapper::impl final {
            public:
                impl()= default;
                ~impl()= default;

                auto cache() -> flat_cache_impl& {
                    return cache_;
                }

            private:
                flat_cache_impl cache_;
            };

            auto dispatcher(
                    flat_cache_impl&cache,
                    api::cache::cache_command&command,
                    api::cache_element output
            ) -> void {

                auto& op = command.key();

                if( "set" == op ){
                    cache.set(command.space(),command.key(),command.value());
                    output = nullptr;
                    return;
                }

                if ( "get" == op ){
                    output = cache.get(command.space(),command.key());
                    return;
                }

            }

            flat_cache_wrapper::flat_cache_wrapper(goblin_engineer::context_t *ctx):
                abstract_service(ctx,"cache"),
                pimpl(std::make_unique<impl>()) {

                attach(
                        actor_zeta::behavior::make_handler(
                            "cache",
                            [this](actor_zeta::behavior::context& ctx) -> void {
                                auto& command = ctx.message().body<api::cache::cache_command>();
                                api::cache_element output ;
                                dispatcher(pimpl->cache(),command,output);
                                if( output.get() != nullptr ) {
                                    ctx.message().sender()->send(
                                            actor_zeta::messaging::make_message(
                                                    ctx->self(),
                                                    "cache",
                                                    std::move(output)

                                            )
                                    );
                                }
                            }
                        )
                );

            }

            void flat_cache_wrapper::startup(goblin_engineer::context_t *) {

            }

            void flat_cache_wrapper::shutdown() {

            }

}}}
