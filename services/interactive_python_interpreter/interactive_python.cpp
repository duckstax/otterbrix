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
        add_handler("start_session", &interactive_python::start_session);
        add_handler("stop_session", &interactive_python::stop_session);
        env->pre_hook([this, env]() {
            async_init(env->zmq_context(),
                       [this](const std::string& socket_name, std::vector<std::string> msg) {
                           auto jupyter = this->addresses("jupyter");
                           actor_zeta::send(jupyter, this->address(), "write", components::buffer(socket_name, msg));
                       });
        });

        log_.info("construct  interactive_python finish");
    }

    auto interactive_python::registration(std::vector<std::string>) -> void {
    }

    auto interactive_python::dispatch_shell(components::zmq_buffer_t& msgs) -> void {
        python_interpreter_->dispatch_shell(msgs->msg());
    }

    auto interactive_python::dispatch_control(components::zmq_buffer_t& msgs) -> void {
        python_interpreter_->dispatch_control(msgs->msg());
    }

    void interactive_python::async_init(zmq::context_t& ctx, std::function<void(const std::string&, std::vector<std::string>)> f) {
        python_interpreter_->init(ctx, std::move(f));
    }

    auto interactive_python::stop_session() -> void {
        log_.info("Run stop_session");
        python_interpreter_->stop_session();
    }

    auto interactive_python::start_session() -> void {
        log_.info("Run start_session");
    }

} // namespace services
