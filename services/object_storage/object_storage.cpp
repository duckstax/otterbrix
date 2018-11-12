#include <rocketjoe/services/object_storage/object_storage.hpp>

#include <rocketjoe/api/transport_base.hpp>
#include <rocketjoe/api/http.hpp>
#include <rocketjoe/api/json_rpc.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/json.hpp>

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_serialize.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <rocketjoe/services/object_storage/object_storage_implement.hpp>

#include <goblin-engineer/context.hpp>
#include <api/application.hpp>


namespace rocketjoe { namespace services { namespace object_storage {

            template<typename Data>
            inline void log(Data &data) {
                std::cerr << __FILE__ << " | " << __LINE__ << " | " << data << std::endl;
            }

            using bsoncxx::builder::basic::kvp;

            class object_storage::impl final : public object_storage_implement {
            public:
                impl() = delete;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

                ~impl() = default;

                impl(const std::string &uri_mongo, const std::string &mongo_root_db)
                    : object_storage_implement(uri_mongo) {
                    root_systems_db = mongo_root_db;
                }

                auto system_database_name() const -> const std::string& {
                    return root_systems_db;
                }

            private:
                std::string root_systems_db;

            };

            void object_storage::shutdown() {

            }

            void object_storage::startup(goblin_engineer::context_t *ctx) {

            }

            object_storage::object_storage(goblin_engineer::context_t *ctx) : abstract_service(ctx,"object_storage") {

                auto mongo_uri = ctx->config().as_object()["mongo-uri"].as_string();
                auto mongo_root_db = ctx->config().as_object()["mongo-root-db"].as_string();
                pimpl = std::make_unique<impl>(mongo_uri, mongo_root_db);

                attach(
                        actor_zeta::behavior::make_handler(
                                "create-app",
                                [this](actor_zeta::behavior::context &ctx) -> void {

                                    const auto& t = ctx.message().body<api::task>();
                                    auto app_name = t.request.params.at("name").get<std::string>();

                                    auto app_id = boost::uuids::to_string(boost::uuids::random_generator_mt19937()());
                                    auto app_key = boost::uuids::to_string(boost::uuids::random_generator_mt19937()());

                                    auto app = bsoncxx::builder::basic::make_document(
                                            kvp("name", app_name),
                                            kvp("application-id", app_id),
                                            kvp("api-key", app_key)
                                    );

                                    auto filter = bsoncxx::builder::basic::make_document(
                                            kvp("name", app_name)
                                    );

                                    /*
                                    auto result = pimpl->replace(
                                            pimpl->system_database_name(),
                                            "applications",
                                            filter.view(),
                                            app.view()
                                    );
                                     */

                                    auto* http = new api::http(t.transport_->id());

                                    api::json_rpc::response_message response;
                                    response.id = t.request.id;
                                    response.result["application-id"] = app_id;
                                    response.result["api-key"] = app_key;

                                    http->body(api::json_rpc::serialize(response));
                                    http->header("Content-Type", "application/json");

                                    api::app_info app_info_(app_name,app_id,app_key);

                                    ctx->addresses("router")->send(
                                            actor_zeta::messaging::make_message(
                                                    ctx->self(),
                                                    "registered_application",
                                                    std::move(app_info_)
                                            )
                                    );

                                    ctx->addresses("http")->send(
                                            actor_zeta::messaging::make_message(
                                                    ctx->self(),
                                                    "add_trusted_url",
                                                    std::move(app_name)
                                            )
                                    );

                                    ctx->addresses("http")->send(
                                            actor_zeta::messaging::make_message(
                                                    ctx->self(),
                                                    "write",
                                                    api::transport(http)
                                            )
                                    );

                                }
                        )
                );

                attach(
                        actor_zeta::behavior::make_handler(
                                "insert",
                                [this](actor_zeta::behavior::context &ctx) -> void {
                                    auto t = ctx.message().body<api::task>();
                                    log("method : create start");

                                    auto &db_name = t.app_info_.application_id;
                                    auto collection_name = t.request.params["collection"].get<std::string>();
                                    auto document_json = t.request.params["document"].dump();

                                    auto document = bsoncxx::from_json(document_json);

                                    //pimpl->insert(db_name,collection_name,,document);

                                    auto http = std::make_unique<api::http>(t.transport_->id());

                                    api::json_rpc::response_message response;
                                    response.id = t.request.id;
                                    http->body(api::json_rpc::serialize(response));
                                    http->header("Content-Type", "application/json");

                                    ctx->addresses("http")->send(
                                            actor_zeta::messaging::make_message(
                                                ctx->self(),
                                                "write",
                                                api::transport(http.release())

                                            )
                                    );

                                    log("method : create finish");
                                }
                        )
                );

                attach(
                        actor_zeta::behavior::make_handler(
                                "update",
                                [this](actor_zeta::behavior::context &ctx) -> void {
                                    auto t = ctx.message().body<api::task>();

                                    log("method : update start");

                                    auto &db_name = t.app_info_.application_id;
                                    auto collection_name = t.request.params["collection"].get<std::string>();
                                    auto document_json = t.request.params["document"].dump();

                                    auto document = bsoncxx::from_json(document_json);
                                    ///pimpl->update(db_name,collection_name,,document.view());

                                    auto *http = new api::http(t.transport_->id());

                                    api::json_rpc::response_message response;
                                    response.id = t.request.id;
                                    http->body(api::json_rpc::serialize(response));
                                    http->header("Content-Type", "application/json");

                                    ctx->addresses("http")->send(
                                            actor_zeta::messaging::make_message(
                                                    ctx->self(),
                                                    "write",
                                                    api::transport(http)

                                            )
                                    );

                                    log("method : update finish");
                                }
                        )
                );


                attach(
                        actor_zeta::behavior::make_handler(
                                "remove",
                                [this](actor_zeta::behavior::context &ctx) -> void {
                                    log("method : delete start");
                                    auto t = ctx.message().body<api::task>();

                                    auto &db_name = t.app_info_.application_id;
                                    auto collection_name = t.request.params["collection"].get<std::string>();
                                    auto document_json = t.request.params["document"].dump();

                                    auto document = bsoncxx::from_json(document_json);

                                    ///pimpl->remove(db_name,collection_name,document);

                                    auto http = std::make_unique<api::http>(t.transport_->id());

                                    api::json_rpc::response_message response;
                                    response.id = t.request.id;
                                    http->body(api::json_rpc::serialize(response));
                                    http->header("Content-Type", "application/json");


                                    ctx->addresses("http")->send(
                                            actor_zeta::messaging::make_message(
                                                    ctx->self(),
                                                    "write",
                                                    api::transport(http.release())

                                            )
                                    );

                                    log("method : delete finish");
                                }
                        )
                );




            }


        }
    }
}

