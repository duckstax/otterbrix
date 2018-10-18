#include <rocketjoe/services/database/database.hpp>

#include <memory>

#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>


namespace rocketjoe { namespace services { namespace database {

            class database::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

            };

            void database::shutdown() {

            }

            void database::startup(goblin_engineer::context_t * ctx) {

            }

            std::string database::name() const {
                return "database";
            }

            void database::metadata(goblin_engineer::metadata_service *metadata) const {
                metadata->name = "database";
            }

            database::database(goblin_engineer::context_t *ctx):pimpl(std::make_unique<impl>()) {


            }

            database::~database() {

            }


}}}

