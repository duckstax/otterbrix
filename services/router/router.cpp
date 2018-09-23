#include <rocketjoe/services/router/router.hpp>

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


namespace rocketjoe { namespace services { namespace router {

            struct task {
                api::transport  transport_;
                api::json_rpc::request_message request;

            };

            class router::impl final {
            public:
                impl(const std::string& uri_mongo ){
                    uri = mongocxx::uri{uri_mongo};
                    client = mongocxx::client{uri};
                    apps_ = client.database("applications");
                }

                mongocxx::database& apps() {
                    return apps_;
                }


                ~impl() = default;

                impl &operator=(impl &&) = default;

                impl(impl &&) = default;

                impl &operator=(const impl &) = default;

                impl(const impl &) = default;

            private:
                mongocxx::instance mongo_inst;
                mongocxx::database apps_;
                mongocxx::uri uri;
                mongocxx::client client;
                mongocxx::options::bulk_write bulk_opts;

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

            router::router(goblin_engineer::context_t *ctx)  {
                auto mongo_uri = ctx->config().as_object()["mongo-uri"].as_string();
                pimpl = std::make_unique<impl>(mongo_uri);

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
                                    std::cerr << __FILE__ << " | " << __LINE__ << " | " << "method : " << request.method << std::endl;
                                    task task_;
                                    task_.request = std::move(request);
                                    task_.transport_ = std::move(t);
                                    send(goblin_engineer::message("router",request.method,{std::move(task_)}));
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
                            auto table = t.request.params.at("name").get<std::string>();
                            this->pimpl->apps().create_collection(table);
                        }
                );

                add(
                        "created-or-insert",
                        [this](goblin_engineer::message &&msg) -> void {

                            auto arg = msg.args[0];
                            auto t = boost::any_cast<task>(arg);
                            auto table = t.request.params.at("table").get<std::string>();



                            auto bulk = this->pimpl->apps().collection(table).create_bulk_write();
//                            bsoncxx::from_json()

///                            mongocxx::model::update_one upsert_op{doc1.view(), doc2.view()};

                            // Set upsert to true: if no document matches {"a": 1}, insert {"a": 2}.
///                            upsert_op.upsert(true);

///                            bulk.append(upsert_op);


///                            auto result = bulk.execute();


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

