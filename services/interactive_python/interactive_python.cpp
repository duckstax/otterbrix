#include "interactive_python.hpp"
#include "add_jupyter.hpp"
#include <components/buffer/zmq_buffer.hpp>
#include <services/jupyter/jupyter.hpp>

#include <actor-zeta/core.hpp>
#include <boost/uuid/random_generator.hpp>
#include <nlohmann/json.hpp>

namespace services {

    namespace nl = nlohmann;

    interactive_python_t::interactive_python_t(
        actor_zeta::intrusive_ptr<jupyter> env
        , const components::python_sandbox_configuration& configuration
        , components::log_t& log)
        : mode_{components::sandbox_mode_t::none}
        , goblin_engineer::abstract_service(env, "python")
        , log_(log.clone()) {
        log_.info("construct  interactive_python start");
        mode_ = configuration.mode_;
        python_interpreter_ = std::make_unique<components::python_t>(configuration, log);
        components::load(*python_interpreter_,*this);
        log_.info("python_interpreter_");
        add_handler("shell", &interactive_python_t::dispatch_shell);
        add_handler("control", &interactive_python_t::dispatch_control);
        add_handler("start_session", &interactive_python_t::start_session);
        add_handler("stop_session", &interactive_python_t::stop_session);
        add_handler("registration", &interactive_python_t::registration);

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
            log.info("   if (configuration.mode_ == components::sandbox_mode_t::jupyter_engine)");
            env->pre_hook(
                [this, env, identifier]() mutable {
                    auto jupyter = this->addresses("jupyter");
                    actor_zeta::send(jupyter, this->address(), "identifier", std::move(identifier));

                    auto result = components::buffer("registration", registration_step_one());
                    env->write(result);
                });
        }

        jupyter_connection_path_ = configuration.jupyter_connection_path_;
        log_.info("construct  interactive_python finish");
    }

    auto interactive_python_t::registration(std::vector<std::string>& msgs) -> void {
        jupyter_kernel_->registration(std::move(msgs));
    }

    auto interactive_python_t::dispatch_shell(components::zmq_buffer_t& msgs) -> void {
        jupyter_kernel_->dispatch_shell(std::move(msgs->msg()));
    }

    auto interactive_python_t::dispatch_control(components::zmq_buffer_t& msgs) -> void {
        jupyter_kernel_->dispatch_control(std::move(msgs->msg()));
    }

    void interactive_python_t::async_init(zmq::context_t& ctx, boost::uuids::uuid identifier, std::function<void(const std::string&, std::vector<std::string>)> f) {
        python_interpreter_->init();
        if (components::sandbox_mode_t::jupyter_kernel == mode_) {
            log_.info("jupyter kernel mode");
            jupyter_kernel_init(ctx, std::move(identifier), std::move(f));
        } else if (components::sandbox_mode_t::jupyter_engine == mode_) {
            log_.info("jupyter engine mode");
            jupyter_engine_init(std::move(identifier), std::move(f));
        } else {
            log_.info("init script mode ");
        }
    }

    auto interactive_python_t::stop_session() -> void {
        log_.info("Run stop_session");
        jupyter_kernel_->stop_session();
    }

    auto interactive_python_t::start_session() -> void {
        log_.info("Run start_session");
    }
    auto interactive_python_t::registration_step_one() -> std::vector<std::string> {
        return jupyter_kernel_->registration();
    }

    auto interactive_python_t::jupyter_kernel_init(zmq::context_t& ctx, boost::uuids::uuid identifier, std::function<void(const std::string&, std::vector<std::string>)> f) -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};
        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;
        connection_file >> configuration;
        log_.info(configuration.dump(4));

        std::string transport{configuration["transport"]};
        std::string ip{configuration["ip"]};
        auto stdin_port = std::to_string(configuration["stdin_port"].get<std::uint16_t>());
        auto stdin_address{transport + "://" + ip + ":" + stdin_port};
        stdin_socket_ = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::router);
        stdin_socket_->setsockopt(ZMQ_LINGER, 1000);
        stdin_socket_->bind(stdin_address);

        auto sm = interactive_python_interpreter::jupyter::make_socket_manager(std::move(f), static_cast<zmq::socket_ref>(*stdin_socket_));
        bool engine_mode = false;
        jupyter_kernel_ = boost::intrusive_ptr<interactive_python_interpreter::jupyter::pykernel>(new interactive_python_interpreter::jupyter::pykernel(
            log_,
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            engine_mode,
            std::move(identifier),
            sm));
    }

    auto interactive_python_t::jupyter_engine_init(boost::uuids::uuid identifier, std::function<void(const std::string&, std::vector<std::string>)> f) -> void {
        std::ifstream connection_file{jupyter_connection_path_.string()};

        if (!connection_file) {
            throw std::logic_error("File jupyter_connection not found");
        }

        nl::json configuration;

        connection_file >> configuration;

        log_.info(configuration.dump(4));

        bool engine_mode = true;
        jupyter_kernel_ = boost::intrusive_ptr<interactive_python_interpreter::jupyter::pykernel>{new interactive_python_interpreter::jupyter::pykernel{
            log_,
            std::move(configuration["key"]),
            std::move(configuration["signature_scheme"]),
            engine_mode,
            std::move(identifier),
            interactive_python_interpreter::jupyter::make_socket_manager(std::move(f))}};
    }
    void interactive_python_t::load(components::python_t&vm) {
        services::interactive_python_interpreter::add_jupyter(vm.module("pyrocketjoe"));
    }

} // namespace services
