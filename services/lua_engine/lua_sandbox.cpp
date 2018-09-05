#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>
#include <api/http.hpp>
#include <api/websocket.hpp>
#include <goblin-engineer/message.hpp>

namespace RocketJoe { namespace services { namespace lua_engine { namespace lua_vm {

                constexpr const char * write = "write";

                auto load_libraries( sol::state&lua) -> void {
                    lua.open_libraries(
                            sol::lib::base,
                            sol::lib::table,
                            sol::lib::package,
                            sol::lib::coroutine,
                            sol::lib::math,
                            sol::lib::utf8,
                            sol::lib::string
                    );
                }


                auto object_to_lua_table (transport::transport& object, sol::table& table) ->void {
                    switch (object.transport_->type()) {
                        case transport::transport_type::http: {
                            auto *http_ = static_cast<transport::http *>(object.transport_.get());
                            table["type"] = "http";
                            table["uri"] = http_->uri_;
                            ///headers
                            auto range = http_->headers();
                            for(;range.first != range.second;++range.first){
                                ///std::cerr << "heder_key = "<<range.first->first <<" header_value "<<range.first->second <<std::endl;
                                table[range.first->first]=range.first->second;
                            }
                            ///TODO Header / body not copy add method json_body_get
                            table["body"] = http_->body_;

                            break;
                        }
                        case transport::transport_type::ws: {
                            table["type"] = "ws";
                            break;
                        }
                    }

                }


                auto lua_table_to_object (sol::table& table,transport::transport& object) ->void {
                    switch (object.transport_->type()) {
                        case transport::transport_type::http: {
                            auto *http_ = static_cast<transport::http *>(object.transport_.get());
                            auto header =  table["header"].get<sol::table>();
                            for(auto&i:header){
                                std::cerr<< "header_key" <<i.first.as<std::string>() << "header_value"<<i.second.as<std::string>()<<std::endl;
                                http_->header(i.first.as<std::string>(),i.second.as<std::string>());

                            }
                            http_->body(table["body"].get<std::string>());

                            break;
                        }
                        case transport::transport_type::ws: {
                            table["type"] = "ws";
                            break;
                        }
                    }

                }

                lua_vm::lua_context::lua_context(const std::string& name,goblin_engineer::pipe* ptr) {
                    pipe = ptr;

                    load_libraries(lua);
                    ///TODO!
                    lua.set_function(
                            "jobs_wait",
                            [this](sol::table jobs) {
                                ///std::cerr<<device_.pop()<<std::endl;
                                auto job_id = device_.pop();
                                if(job_id != 0){
                                    std::cerr<<job_id<<std::endl;
                                    jobs[1] = job_id;
                                }

                            }
                    );
                    /// C++ -> lua owener
                    lua.set_function(
                            "job_read",
                            [this](std::size_t id,sol::table response) -> void {
                                if (device_.in(id)) {
                                    auto &transport = device_.get(id);
                                    object_to_lua_table(transport,response);
                                }
                            }
                    );

                    ////lua -> C++ owener
                    lua.set_function(
                            "job_write_and_close",
                            [this](sol::table response){
                                auto id =  response["id"].get<size_t >();
                                auto type =  response["type"].get<std::string>();

                                auto in_put = device_.get(id).transport_->id();
                                std::cerr<<"id = " <<id << "type = "<<type<<std::endl;

                                if("http" == type){
                                    auto http = transport::make_transport<transport::http>(in_put);
                                    lua_table_to_object(response,http);

                                    pipe->send(goblin_engineer::message("http",write,{std::move(http)}));
                                } else if ("ws" == type){
                                    auto ws = transport::make_transport<transport::web_socket>(id);
                                    lua_table_to_object(response,ws);
                                    pipe->send(goblin_engineer::message("ws",write,{std::move(ws)}));
                                }



                            }
                    );

                    r = lua.load_file(name);

                }

                auto lua_context::run() -> void {
                    exuctor = std::make_unique<std::thread>(
                            [this]() {
                                r();
                            }
                    );
                }

                auto lua_context::push_job(RocketJoe::transport::transport &&job) -> void {
                    device_.push(std::move(job));
                }

}}}}