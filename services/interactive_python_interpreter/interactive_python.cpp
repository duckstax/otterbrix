#include "interactive_python.hpp"
#include <components/buffer/zmq_buffer.hpp>
#include <services/jupyter/jupyter.hpp>

#include <boost/uuid/random_generator.hpp>

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
        add_handler("registration", &interactive_python::registration);

        auto identifier = boost::uuids::random_generator()();

        /// TODO: hack
        env->pre_hook(
            [this, env, identifier]() {
                async_init(env->zmq_context(),
                           std::move(identifier),
                           [this](const std::string& socket_name, std::vector<std::string> msg) {
                               auto jupyter = this->addresses("jupyter");
                               actor_zeta::send(jupyter, this->address(), "write", components::buffer(socket_name, msg));
                           });
            });

        /// TODO: hack
        if (configuration.mode_ == components::sandbox_mode_t::jupyter_engine) {
            env->pre_hook(
                [this, env, identifier]() mutable {
                  auto jupyter = this->addresses("jupyter");
                  actor_zeta::send(jupyter, this->address(), "identifier", std::move(identifier));

                  auto result = components::buffer("registration", python_interpreter_->registration());
                  env->write(result);
                });
        }

        log_.info("construct  interactive_python finish");
    }

    auto interactive_python::registration(std::vector<std::string> msgs) -> void {
        python_interpreter_->registration(msgs);
    }

    auto interactive_python::dispatch_shell(components::zmq_buffer_t& msgs) -> void {
        python_interpreter_->dispatch_shell(msgs->msg());
    }

    auto interactive_python::dispatch_control(components::zmq_buffer_t& msgs) -> void {
        python_interpreter_->dispatch_control(msgs->msg());
    }

    void interactive_python::async_init(zmq::context_t& ctx, boost::uuids::uuid identifier, std::function<void(const std::string&, std::vector<std::string>)> f) {
        python_interpreter_->init(ctx, std::move(identifier), std::move(f));
    }

    auto interactive_python::stop_session() -> void {
        log_.info("Run stop_session");
        python_interpreter_->stop_session();
    }

    auto interactive_python::start_session() -> void {
        log_.info("Run start_session");
    }

} // namespace services
