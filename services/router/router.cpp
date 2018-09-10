#include <rocketjoe/services/router/router.hpp>

#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>
#include <rocketjoe/api/transport_base.hpp>


namespace RocketJoe {
    namespace services {
        namespace router {

            class router::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

            };

            void router::shutdown() {

            }

            void router::startup(goblin_engineer::context_t *ctx) {

            }

            std::string router::name() const {
                return "router";
            }

            void router::metadata(goblin_engineer::metadata_service *metadata) const {
                metadata->name = "router";
            }

            router::router(goblin_engineer::context_t *ctx) : pimpl(std::make_unique<impl>()) {

                add(
                        "dispatcher",
                        [this](goblin_engineer::message &&msg) -> void {
                            auto arg = msg.args[0];
                            auto t = boost::any_cast<transport::transport>(arg);
                        }
                );

                add(
                        "create-app",
                        [this](goblin_engineer::message &&msg) -> void {

                        }
                );

                add(
                        "created-or-insert",
                        [this](goblin_engineer::message &&msg) -> void {

                        }
                );

                add(
                        "find",
                        [this](goblin_engineer::message &&msg) -> void {

                        }
                );


            }

            router::~router() = default;


        }
    }
}

