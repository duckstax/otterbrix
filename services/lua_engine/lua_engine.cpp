#include <rocketjoe/services/lua_engine/lua_engine.hpp>

#include <map>

#include <boost/utility/string_view.hpp>
#include <boost/filesystem.hpp>

#include <sol.hpp>

#include <goblin-engineer/metadata.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <goblin-engineer/context.hpp>

#include <actor-zeta/actor/actor_address.hpp>

#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

            class lua_engine::impl final : public lua_context  {
            public:

                explicit impl(actor_zeta::behavior::context_t& ptr):
                    lua_context(ptr){
                }

                impl() = default;

                ~impl() override = default;

            };

            /// TODO: UB
            lua_engine::lua_engine(goblin_engineer::context_t * context):
                abstract_service(context,"lua_engine"),
                pimpl(new impl(*this)) {

                    attach(
                            actor_zeta::behavior::make_handler(
                                    "dispatcher",
                                    [this](actor_zeta::behavior::context &ctx) -> void {
                                        auto t = ctx.message().body<api::transport>();
                                        pimpl->push_job(std::move(t));
                                    }
                            )
                    );

                /// TODO: Move to startup()

                std::cerr << "processing env lua start " <<std::endl;

                boost::filesystem::path config_path(context->config().as_object()["config-path"].as_string());

                auto&env_lua = context->config().as_object()["env-lua"].as_object();

                std::map<std::string,std::string> env ;

                bool check_init = false;

                for( auto &i : env_lua ){
                    std::cerr << i.first << " : "  << (config_path/i.second.as_string()).string()<<std::endl;

                    if(boost::filesystem::exists(config_path/i.second.as_string())){

                        check_init = true;
                        env.emplace(i.first,(config_path/i.second.as_string()).string());

                    } else {

                        check_init = false ;
                        break;
                    }

                }

                if(check_init) {
                    pimpl->environment_configuration(context->config().as_object().at("app").as_string(),env);
                    pimpl->run();
                } else {
                    std::cerr << " Error env lua " << std::endl;
                }

                std::cerr << "processing env lua finish " <<std::endl;
            }

            void lua_engine::startup(goblin_engineer::context_t * context) {

               /// TODO: FIX cycle init service
            }

            void lua_engine::shutdown() {

            }

}}}