#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>
#include <map>
#include <api/http.hpp>
#include <api/websocket.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

            constexpr const char *write = "write";

            auto load_libraries(sol::state &lua, const std::map<std::string, std::string> &env) -> void {

                lua.open_libraries(
                        sol::lib::base,
                        sol::lib::table,
                        sol::lib::package,
                        sol::lib::coroutine,
                        sol::lib::math,
                        sol::lib::utf8,
                        sol::lib::string
                );

                for (const auto &i:env) {
                    lua.require_file(i.first, i.second);
                }

            }

/*
            auto object_to_lua_table(api::transport &object, sol::table &table) -> void {
                switch (object->type()) {
                    case api::transport_type::http: {
                        auto *http_ = static_cast<api::http *>(object.get());
                        table["type"] = "http";
                        table["uri"] = http_->uri();
                        ///headers
                        for (auto &i :*http_) {
                            ///std::cerr << "heder_key = "<< i.first <<" header_value "<< i.second <<std::endl;
                            table[i.first] = i.second;
                        }
                        ///TODO Header / body not copy add method json_body_get
                        table["body"] = http_->body();

                        break;
                    }
                    case api::transport_type::ws: {
                        table["type"] = "ws";
                        break;
                    }
                }

            }


            auto lua_table_to_object(sol::table &table, api::transport &object) -> void {
                switch (object->type()) {
                    case api::transport_type::http: {
                        auto *http_ = static_cast<api::http *>(object.get());
                        auto header = table["header"].get<sol::table>();
                        for (auto &i:header) {
                            std::cerr << "header_key" << i.first.as<std::string>() << "header_value"
                                      << i.second.as<std::string>() << std::endl;
                            http_->header(i.first.as<std::string>(), i.second.as<std::string>());

                        }
                        http_->body(table["body"].get<std::string>());

                        break;
                    }
                    case api::transport_type::ws: {
                        table["type"] = "ws";
                        break;
                    }
                }

            }
*/
            lua_context::lua_context(actor_zeta::behavior::context_t &ptr) : context_(ptr) {

            }

            auto lua_context::run() -> void {
                exuctor = std::make_unique<std::thread>(
                        [this]() {
                            r();
                        }
                );
            }

            auto lua_context::push_job(api::transport &&job) -> void {
                device_.push(std::move(job));
            }

            auto lua_context::environment_configuration(
                    const std::string &name,
                    const std::map<std::string, std::string> &env
            ) -> void {

                load_libraries(lua, env);

                lua.set_function(
                        "jobs_wait",
                        [this](sol::table jobs) -> std::size_t {

                            auto size = device_.pop_all(jobs_id);

                            if (size != 0) {

                                for (auto i = 0; i <= size; ++i) {
                                    jobs[i] = jobs_id[i];
                                }

                            }

                            return size;

                        }
                );

                lua.set_function(
                        "jobs_type",
                        [this](std::size_t id) -> bool {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                return bool(transport->type());
                            }


                        }
                );


                lua.set_function(
                        "http_close",
                        [this](std::size_t id) -> std::string {
                            if (device_.in(id)) {


                            }
                        }
                );


                lua.set_function(
                        "http_header",
                        [this](std::size_t id, std::string name) -> std::string {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                if (api::transport_type::http == transport->type()) {
                                    auto *http = static_cast<api::http *>(transport.get());
                                    return http->header(name);
                                }

                            }
                        }
                );

                lua.set_function(
                        "http_header",
                        [this](std::size_t id, std::string name, std::string value) -> void {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                if (api::transport_type::http == transport->type()) {
                                    auto *http = static_cast<api::http *>(transport.get());
                                    http->header(std::move(name), std::move(value));
                                }

                            }
                        }
                );

                lua.set_function(
                        "http_uri",
                        [this](std::size_t id) -> std::string {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                if (api::transport_type::http == transport->type()) {
                                    auto *http = static_cast<api::http *>(transport.get());
                                    return http->uri();
                                }
                            }
                        }
                );

                lua.set_function(
                        "http_body",
                        [this](std::size_t id) -> std::string {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                if (api::transport_type::http == transport->type()) {
                                    auto *http = static_cast<api::http *>(transport.get());
                                    return http->body();
                                }
                            }
                        }
                );

                lua.set_function(
                        "http_method",
                        [this](std::size_t id) -> std::string {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                if (api::transport_type::http == transport->type()) {
                                    auto *http = static_cast<api::http *>(transport.get());
                                    return http->method();
                                }

                            }
                        }
                );


                lua.set_function(
                        "http_status",
                        [this](std::size_t id) -> std::size_t {
                            if (device_.in(id)) {
                                auto &transport = device_.get(id);
                                if (api::transport_type::http == transport->type()) {
                                    auto *http = static_cast<api::http *>(transport.get());
                                    return http->status();
                                }

                            }
                        }
                );


                r = lua.load_file(name);
            }

}}}