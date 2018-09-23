#include <rocketjoe/services/database/database.hpp>

#include <memory>

#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

#include <mongocxx/cursor.hpp>

namespace rocketjoe { namespace services { namespace database {

            using bsoncxx::builder::stream::document;
            using bsoncxx::builder::stream::open_document;

            class database::impl final {
            public:
                impl() = default;

                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

                // Mongo connection members

                /*
                mongocxx::instance mongo_inst;
                mongocxx::database mongo_database;
                mongocxx::uri uri;
                mongocxx::client client;
                mongocxx::options::bulk_write bulk_opts;

                 */
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
                //auto mongo_uri = ctx->config().as_object()["mongo-uri"].as_string();
                //pimpl->uri = mongocxx::uri{mongo_uri};
                //pimpl->client = mongocxx::client{pimpl->uri};

/*
                auto& client =  pimpl->client;

                mongocxx::database database = client.database("system");
                mongocxx::collection collection = database["config"];

                mongocxx::database apps = client.database("applications");

*/

            }

            database::~database() {

            }


}}}

