#include <rocketjoe/services/resource_manager/resource_manager.hpp>

#include <memory>

#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>


namespace rocketjoe { namespace services { namespace resource_manager {

            class resource_manager::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

            };

            void resource_manager::shutdown() {

            }

            void resource_manager::startup(goblin_engineer::context_t * ctx) {

            }

            resource_manager::resource_manager(goblin_engineer::context_t *ctx):
                abstract_service(ctx,"resource_manager"),
                pimpl(std::make_unique<impl>()) {


            }

            resource_manager::~resource_manager() {

            }


}}}

