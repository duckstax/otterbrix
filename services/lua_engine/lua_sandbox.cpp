#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

#include <boost/filesystem.hpp>

#include <api/http.hpp>
#include <api/websocket.hpp>

#include <goblin-engineer/dynamic.hpp>

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


    auto lua_context::run() -> void {
        exuctor = std::make_unique<std::thread>(
                [this]() {
                    sol::load_result r = lua.load_file(this->path_script);
                    r();
                }
        );
    }

    auto lua_context::push_job(api::transport &&job) -> void {
        device_.push(std::move(job));
    }

    lua_context::lua_context(
            goblin_engineer::dynamic_config &configuration,
            actor_zeta::actor::actor_address ptr) :
            address(std::move(ptr)) {

        std::cerr << "processing env lua start " << std::endl;

        path_script = configuration.as_object().at("app").as_string();

        boost::filesystem::path config_path(configuration.as_object()["config-path"].as_string());

        auto &env_lua = configuration.as_object()["env-lua"].as_object();

        std::map<std::string, std::string> env;


        for (auto &i : env_lua) {
            std::cerr << i.first << " : " << (config_path / i.second.as_string()).string() << std::endl;

            if (boost::filesystem::exists(config_path / i.second.as_string())) {

                env.emplace(i.first, (config_path / i.second.as_string()).string());

            }

        }

        std::cerr << "processing env lua finish " << std::endl;


        load_libraries(lua, env);

        /// read
        lua.set_function(
                "jobs_wait",
                [this](sol::table jobs) -> std::size_t {

                    int size = device_.pop_all(jobs_id);

                    if (size != 0) {

                        for (int i = 0; i <= size - 1; ++i) {
                            jobs[i] = jobs_id[i];
                        }

                    }

                    return size;

                }
        );

        /// read
        lua.set_function(
                "job_type",
                [this](std::size_t id) -> uint {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        return uint(transport->type());
                    }

                }
        );

        /// write
        lua.set_function(
                "job_close",
                [this](std::size_t id) {
                    if (device_.in(id)) {
                        auto result = std::move(device_.release(id));

                        address->send(
                                actor_zeta::messaging::make_message(
                                        address->address(),
                                        "write",
                                        std::move(result)
                                )
                        );

                    }
                }
        );

        /// read
        lua.set_function(
                "http_header_read",
                [this](std::size_t id, std::string name) -> std::string {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            return http->header(name);
                        }

                    }
                }
        );

        /// write
        lua.set_function(
                "http_header",
                [this](std::size_t id, std::string name, std::string value) -> void {
                    if (device_.in(id)) {
                        auto &transport = device_.get_second(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            http->header(std::move(name), std::move(value));
                        }

                    }
                }
        );
        /// read
        lua.set_function(
                "http_uri",
                [this](std::size_t id) -> std::string {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            return http->uri();
                        }
                    }
                }
        );

        /// write
        lua.set_function(
                "http_body_write",
                [this](std::size_t id, std::string body_) {

                    if (device_.in(id)) {
                        auto &transport = device_.get_second(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            return http->body(body_);
                        }
                    }

                }
        );

        /// read
        lua.set_function(
                "http_body_read",
                [this](std::size_t id) -> std::string {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            return http->body();
                        }
                    }
                }
        );

        /// read
        lua.set_function(
                "http_method",
                [this](std::size_t id) -> std::string {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            return http->method();
                        }

                    }
                }
        );

        /// read
        lua.set_function(
                "http_status",
                [this](std::size_t id) -> std::size_t {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        if (api::transport_type::http == transport->type()) {
                            auto *http = static_cast<api::http *>(transport.get());
                            return http->status();
                        }

                    }
                }
        );
    }

}}}