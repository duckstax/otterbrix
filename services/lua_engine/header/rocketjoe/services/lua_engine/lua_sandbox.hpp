#pragma  once

#include <array>
#include <map>

#include <rocketjoe/services/lua_engine/device.hpp>
#include <rocketjoe/api/transport_base.hpp>
#include <sol.hpp>
#include <goblin-engineer/abstract_service.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

                class lua_context {
                public:

                    explicit lua_context(actor_zeta::behavior::context_t&);

                    virtual ~lua_context() = default;

                    auto environment_configuration(const std::string&,const std::map<std::string,std::string>&) -> void ;

                    auto push_job(api::transport &&) -> void;

                    auto run() -> void;

                private:
                    std::vector<std::size_t> jobs_id;
                    device<api::transport> device_;
                    sol::environment environment;
                    sol::state lua;
                    sol::load_result r;
                    actor_zeta::behavior::context_t& context_;
                    std::unique_ptr<std::thread> exuctor;  ///TODO: HACK
                };

}}}