#include "interactive_python.hpp"
#include <services/jupyter/jupyter.hpp>
namespace services {

    interactive_python::interactive_python(
        actor_zeta::intrusive_ptr<jupyter> env,
        const components::python_sandbox_configuration& configuration,
        components::log_t& log)
        : goblin_engineer::abstract_service(env, "python")
        , log_(log.clone()) {
        python_interpreter_ = std::make_unique<components::python_interpreter>(env->zmq_context(), configuration, log, [this](const std::string& socket_name, std::vector<std::string> msg) {
            auto jupyter = this->addresses("jupyter");
            if (jupyter) {
                actor_zeta::send(this->address(), jupyter, "write", buffer(socket_name, msg));
            } else {
                throw std::runtime_error("not jupyter pointer");
            }
        });

        add_handler("shell", &interactive_python::dispatch_shell);
        add_handler("control", &interactive_python::dispatch_control);
    }

    auto interactive_python::registration(std::vector<std::string>) -> void {
    }

    auto interactive_python::dispatch_shell(std::vector<std::string> msgs) -> void {
        python_interpreter_->dispatch_shell(msgs);
    }

    auto interactive_python::dispatch_control(std::vector<std::string> msgs) -> void {
        python_interpreter_->dispatch_control(msgs);
    }

} // namespace services
