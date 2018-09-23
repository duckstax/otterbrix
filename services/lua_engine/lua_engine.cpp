#include <rocketjoe/services/lua_engine/lua_engine.hpp>

#include <boost/utility/string_view.hpp>


#include <rocketjoe/services/lua_engine/lua_sandbox.hpp>

#include <sol.hpp>
#include <goblin-engineer/metadata.hpp>
#include <goblin-engineer/message.hpp>
#include <goblin-engineer/dynamic.hpp>
#include <goblin-engineer/context.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

            class lua_engine::impl final {
            public:
                impl() = default;

                ~impl() = default;

                auto init_vm(std::string name,goblin_engineer::pipe* ptr) -> void {
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

            lua_engine::lua_engine(goblin_engineer::context_t * context):pimpl(std::make_unique<impl>()) {
                pimpl->init_vm(context->config().as_object().at("app").as_string(),to_pipe());

                add(
                        "dispatcher",
                        [this](goblin_engineer::message && message) -> void  {
                            auto arg = std::move(message.args[0]);
                            auto t = std::move(boost::any_cast<api::transport>(arg));
                            pimpl->push_job(std::move(t));
                        }
                );
            }

            lua_engine::~lua_engine() = default;
            
            void lua_engine::metadata(goblin_engineer::metadata_service*metadata) const {
                metadata->name = "lua_engine";

            }

            void lua_engine::startup(goblin_engineer::context_t * context) {
                pimpl->load_vm();
            }

            void lua_engine::shutdown() {

            }

            std::string lua_engine::name() const {
                return std::string("lua_engine");
            }

        }
    }
}