#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

#include <iostream>

#include <boost/filesystem.hpp>

#include <rocketjoe/network/network.hpp>
#include <goblin-engineer.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

    constexpr const char *write = "write";

    enum class transport_type : unsigned char {
                http = 0x00,
                ws = 0x01,
    };

    int my_exception_handler(lua_State *L, sol::optional<const std::exception &> maybe_exception,sol::string_view description) {
        std::cerr << "An exception occurred in a function, here's what it says ";
        if (maybe_exception) {
            std::cerr << "(straight from the exception): ";
            const std::exception &ex = *maybe_exception;
            std::cerr << ex.what() << std::endl;
        } else {
            std::cerr << "(from the description parameter): ";
            std::cerr.write(description.data(), description.size());
            std::cerr << std::endl;
        }

        return sol::stack::push(L, description);
    }

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

        lua.set_exception_handler(&my_exception_handler);


    }


    auto lua_context::run() -> void {
        if(path_script.extension() == ".lua") {
            exuctor = std::make_unique<std::thread>(
                    [this]() {
                        auto r = lua.load_file(this->path_script.string());
                        r();
                    }
            );
        }
    }

    auto lua_context::push_job(network::query_context &&job) -> void {
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
                    return device_.pop_all(jobs);
                }
        );

        /// read
        lua.set_function(
                "job_type",
                [this](std::size_t id) -> uint {
                    if (device_.in(id)) {
                        auto &transport = device_.get_first(id);
                        return uint(transport_type::http);
                    }

                }
        );

        /// write
        lua.set_function(
                "job_close",
                [this](std::size_t id) {
                    if (device_.in(id)) {
                       device_.release(id);
                    }
                }
        );

        /// read
        lua.set_function(
                "http_header_read",
                [this](std::size_t id, std::string name) -> std::string {
                    if (device_.in(id)) {
                        auto &http = device_.get_first(id);
                        return http[name].to_string();
                    }
                }
        );

        /// write
        lua.set_function(
                "http_header",
                [this](std::size_t id, std::string name, std::string value) -> void {
                    if (device_.in(id)) {
                        auto &http = device_.get_second(id);
                        http.set(name,value);
                    }
                }
        );
        /// read
        lua.set_function(
                "http_uri",
                [this](std::size_t id) -> std::string {
                    if (device_.in(id)) {
                        auto &http = device_.get_first(id);
                        return http.target().to_string();
                    }
                }
        );

        /// write
        lua.set_function(
                "http_body_write",
                [this](std::size_t id, std::string body_) {

                    if (device_.in(id)) {
                        auto &http = device_.get_second(id);
                        return http.body()=body_;
                    }

                }
        );

        /// read
        lua.set_function(
                "http_body_read",
                [this](std::size_t id) -> std::string {
                    if (device_.in(id)) {
                        auto &http = device_.get_first(id);
                        return http.body();
                    }
                }
        );

        /// read
        lua.set_function(
                "http_method",
                [this](std::size_t id) -> std::string {
                    if (device_.in(id)) {
                        auto &http = device_.get_first(id);
                        return http.method_string().to_string();
                    }
                }
        );

        /// read
        lua.set_function(
                "http_status",
                [this](std::size_t id) -> std::size_t {
                    if (device_.in(id)) {
                        auto &http = device_.get_second(id);
                        http.result(network::http::status::ok);
                    }
                }
        );
    }

}}}