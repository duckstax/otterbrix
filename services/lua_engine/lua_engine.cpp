#include <rocketjoe/services/lua_engine/lua_engine.hpp>

#include <boost/utility/string_view.hpp>


#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

#include <sol.hpp>
#include <goblin-engineer/metadata.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <goblin-engineer/context.hpp>

#include <actor-zeta/actor/actor_address.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

            class lua_engine::impl final {
            public:
                impl() = default;

                ~impl() = default;

                auto init_vm(std::string name,actor_zeta::behavior::context_t& ptr) -> void {
                    vm_ = std::make_unique<lua_vm::lua_context>(name,ptr);
                }

                auto load_vm() -> void {
                    vm_->run();
                }

                template<typename Job>
                auto push_job(Job&& job){
                    vm_->push_job(std::forward<Job>(job));
                }

            private:
                std::unique_ptr<lua_vm::lua_context> vm_;
            };

            lua_engine::lua_engine(goblin_engineer::context_t * context):
                abstract_service(context,"lua_engine"),
                pimpl(new impl) {
                    attach(
                            actor_zeta::behavior::make_handler(
                            "dispatcher",
                            [this](actor_zeta::behavior::context &ctx) -> void  {
                                std::cerr<<"9999"<<std::endl;
                                auto t = ctx.message().body<api::transport>();
                                pimpl->push_job(std::move(t));
                            }
                    ));

            }

            lua_engine::~lua_engine() = default;

            void lua_engine::startup(goblin_engineer::context_t * context) {
                pimpl->init_vm(context->config().as_object().at("app").as_string(), *this);
                pimpl->load_vm();
            }

            void lua_engine::shutdown() {

            }

}}}