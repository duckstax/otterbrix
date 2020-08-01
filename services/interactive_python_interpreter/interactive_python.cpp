#include "interactive_python.hpp"
#include <components/buffer/zmq_buffer.hpp>
#include <services/jupyter/jupyter.hpp>
namespace services {

    interactive_python::interactive_python(
        actor_zeta::intrusive_ptr<jupyter> env,
        const components::python_sandbox_configuration& configuration,
        components::log_t& log)
        : goblin_engineer::abstract_service(env, "python")
        , log_(log.clone()) {
        log_.info("construct  interactive_python start");
        python_interpreter_ = std::make_unique<components::python_interpreter>(configuration, log);
        log_.info("python_interpreter_");
        add_handler("shell", &interactive_python::dispatch_shell);
        add_handler("control", &interactive_python::dispatch_control);
        env->pre_init([this,env]() {
            async_init(env->zmq_context(),
                       [this](const std::string& socket_name, std::vector<std::string> msg) {
                           auto jupyter = this->addresses("jupyter");
                           if (jupyter) {
                               actor_zeta::send(this->address(), jupyter, "write", components::buffer(socket_name, msg));
                           } else {
                               throw std::runtime_error("not jupyter pointer");
                           }
                       });
        });
        log_.info("construct  interactive_python finish");
    }

    auto interactive_python::registration(std::vector<std::string>) -> void {
    }

    auto interactive_python::dispatch_shell(std::vector<std::string> msgs) -> void {
        python_interpreter_->dispatch_shell(msgs);
    }

    auto interactive_python::dispatch_control(std::vector<std::string> msgs) -> void {
        python_interpreter_->dispatch_control(msgs);
    }

    void interactive_python::async_init(zmq::context_t& ctx, std::function<void(const std::string&, std::vector<std::string>)> f) {
        for(auto&i:message_types()){
           log_.info(i);
        }
        python_interpreter_->init(ctx, std::move(f));
    }

} // namespace services
