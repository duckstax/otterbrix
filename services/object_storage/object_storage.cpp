#include <rocketjoe/services/object_storage/object_storage.hpp>

#include <goblin-engineer/message.hpp>
#include <goblin-engineer/context.hpp>
#include <goblin-engineer/metadata.hpp>

#include <rocketjoe/api/transport_base.hpp>
#include <rocketjoe/api/http.hpp>
#include <rocketjoe/api/json_rpc.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/bulk_write.hpp>

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_serialize.hpp>
#include <boost/uuid/uuid_io.hpp>


namespace rocketjoe { namespace services { namespace object_storage {

            template<typename Data>
            inline void log(Data& data){
                std::cerr << __FILE__ << " | " << __LINE__ << " | " << data << std::endl;
            }

            using bsoncxx::builder::basic::kvp;

            struct task final {
                api::transport  transport_;
                api::json_rpc::request_message request;

            };

            class object_storage::impl final {
            public:
                impl() = delete;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

                ~impl() = default;

                impl(const std::string& uri_mongo,const std::string& mongo_root_db ){
                    root_systems_db = mongo_root_db;
                    uri = mongocxx::uri{uri_mongo};
                    client = mongocxx::client{uri};
                }

                mongocxx::database& find_and_create_database(const std::string&name) {

                    auto db = database_storage.find(name);

                    if( db == database_storage.end()){

                        auto result  = database_storage.emplace(name,client.database(name));
                        return result.first->second;
                    } else {
                        return db->second;
                    }

                }

                mongocxx::collection find_and_create_collection(const std::string&db_name,const std::string&collection) {

                    auto& db = find_and_create_database(db_name);

                    if( !db.has_collection(collection) ){
                        return  db.create_collection(collection);
                    } else {
                        return  db.collection(collection);
                    }

                }

                mongocxx::database& system_database() {
                    return find_and_create_database(root_systems_db);
                }


                mongocxx::collection system_collection(const std::string&collection) {
                    return find_and_create_collection(root_systems_db,collection);
                }


                /// application-id -> database name;
                std::unordered_map<std::string,std::string>resolver;

            private:
                mongocxx::instance mongo_inst;
                std::unordered_map<std::string,mongocxx::database> database_storage;

                mongocxx::uri uri;
                mongocxx::client client;
                mongocxx::options::bulk_write bulk_opts;
                ///std::string app_name_collections = "applications";
                std::string root_systems_db;

            };

            void object_storage::shutdown() {

            }

            void object_storage::startup(goblin_engineer::context_t *ctx) {

            }

            std::string object_storage::name() const {
                return "object_storage_w";
            }

            void object_storage::metadata(goblin_engineer::metadata_service *metadata) const {
                metadata->name = "object_storage_w";
            }

            object_storage::object_storage(goblin_engineer::context_t *ctx)  {

                auto mongo_uri = ctx->config().as_object()["mongo-uri"].as_string();
                auto mongo_root_db = ctx->config().as_object()["mongo-root-db"].as_string();
                pimpl = std::make_unique<impl>(mongo_uri,mongo_root_db);

                add(
                        "dispatcher",
                        [this](goblin_engineer::message &&msg) -> void {
                            auto arg = msg.args[0];

                            auto t = boost::any_cast<api::transport>(arg);
                            switch (t.transport_->type()){
                                case api::transport_type::http:{
                                    auto* http = static_cast<api::http*>(t.transport_.get());
                                    api::json_rpc::request_message request;
                                    api::json_rpc::parse(http->body(),request);
                                    log(std::string("method : ").append(request.method ));
                                    task task_;
                                    task_.request = std::move(request);
                                    task_.transport_ = std::move(t);
                                    send(goblin_engineer::message("object_storage_w",request.method,{std::move(task_)}));
                                    return;
                                }

                                case api::transport_type::ws:{

                                    return;
                                }

                            }
                        }
                );

                add(
                        "create-app",
                        [this](goblin_engineer::message &&msg) -> void {

                            auto arg = msg.args[0];
                            auto t = boost::any_cast<task>(arg);
                            auto app_name = t.request.params.at("name").get<std::string>();

                            mongocxx::collection collection = pimpl->system_collection("applications");

                            auto app_id = boost::uuids::random_generator_mt19937()();
                            auto app_key = boost::uuids::random_generator_mt19937()();

                            auto app = bsoncxx::builder::basic::make_document(
                                    kvp("name",app_name),
                                    kvp("application-id",boost::uuids::to_string(app_id)),
                                    kvp("api-key",boost::uuids::to_string(app_key))
                            );

                            auto filter = bsoncxx::builder::basic::make_document(
                                    kvp("name",app_name)
                            );

                            mongocxx::options::update options;

                            options.upsert(true);

                            auto  result = collection.replace_one(filter.view(),app.view(),options);

                            auto* http = new api::http(t.transport_.transport_->id());

                            api::json_rpc::response_message response;
                            response.id = t.request.id;
                            response.result["application-id"]  = boost::uuids::to_string(app_id);
                            response.result["api-key"]  = boost::uuids::to_string(app_key);

                            http->body(api::json_rpc::serialize(response));
                            http->header("Content-Type","application/json");

                            pimpl->resolver.emplace(boost::uuids::to_string(app_id),app_name);

                            send(goblin_engineer::message("http","add_trusted_url",{std::move(app_name)}));
                            send(goblin_engineer::message("http","write",{api::transport(http)}));

                        }
                );

                add(
                        "create",
                        [this](goblin_engineer::message &&msg) -> void {
                            log("method : create start");

                            auto arg = msg.args[0];
                            auto t = boost::any_cast<task>(arg);

                            auto& db_name = pimpl->resolver.at(t.request.application_id);
                            auto collection_name = t.request.params["collection"].get<std::string>();
                            auto document_json = t.request.params["document"].dump();


                            auto collection = pimpl->find_and_create_collection(db_name,collection_name);
                            auto document = bsoncxx::from_json(document_json);
                            collection.insert_one(document.view());

                            auto* http = new api::http(t.transport_.transport_->id());

                            api::json_rpc::response_message response;
                            response.id = t.request.id;
                            http->body(api::json_rpc::serialize(response));
                            http->header("Content-Type","application/json");

                            send(goblin_engineer::message("http","write",{api::transport(http)}));
                            log("method : create finish");
                        }
                );



                add(
                        "delete",
                        [this](goblin_engineer::message &&msg) -> void {
                            log("method : delete start");

                            auto arg = msg.args[0];
                            auto t = boost::any_cast<task>(arg);

                            auto& db_name = pimpl->resolver.at(t.request.application_id);
                            auto collection_name = t.request.params["collection"].get<std::string>();
                            auto document_json = t.request.params["document"].dump();

                            auto collection = pimpl->find_and_create_collection(db_name,collection_name);
                            auto document = bsoncxx::from_json(document_json);
                            collection.insert_one(document.view());

                            auto* http = new api::http(t.transport_.transport_->id());

                            api::json_rpc::response_message response;
                            response.id = t.request.id;
                            http->body(api::json_rpc::serialize(response));
                            http->header("Content-Type","application/json");

                            send(goblin_engineer::message("http","write",{api::transport(http)}));
                            log("method : delete finish");
                        }
                );


                add(
                        "update",
                        [this](goblin_engineer::message &&msg) -> void {
                            log("method : update start");

                            auto arg = msg.args[0];
                            auto t = boost::any_cast<task>(arg);

                            auto& db_name = pimpl->resolver.at(t.request.application_id);
                            auto collection_name = t.request.params["collection"].get<std::string>();
                            auto document_json = t.request.params["document"].dump();


                            auto collection = pimpl->find_and_create_collection(db_name,collection_name);
                            auto document = bsoncxx::from_json(document_json);
                            collection.insert_one(document.view());

                            auto* http = new api::http(t.transport_.transport_->id());

                            api::json_rpc::response_message response;
                            response.id = t.request.id;
                            http->body(api::json_rpc::serialize(response));
                            http->header("Content-Type","application/json");

                            send(goblin_engineer::message("http","write",{api::transport(http)}));
                            log("method : update finish");
                        }
                );




            }

            object_storage::~object_storage() = default;


        }
    }
}

