#pragma once

#include <boost/uuid/uuid.hpp>

#include <components/buffer/zmq_buffer.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/python/python.hpp>

#include <services/interactive_python/jupyter/pykernel.hpp>

#include <goblin-engineer.hpp>

#include <zmq.hpp>

namespace services {

    class jupyter;

    class interactive_python_t final : public goblin_engineer::abstract_service {
    public:
        interactive_python_t(
            actor_zeta::intrusive_ptr<jupyter>,
            const components::python_sandbox_configuration&,
            components::log_t&);

        auto registration_step_one() -> std::vector<std::string>;

        auto registration(std::vector<std::string>&) -> void;

        auto dispatch_shell(components::zmq_buffer_t&) -> void;

        auto dispatch_control(components::zmq_buffer_t&) -> void;

        auto stop_session() -> void;

        auto start_session() -> void;

        void async_init(zmq::context_t&, boost::uuids::uuid identifier, std::function<void(const std::string&, std::vector<std::string>)>);

        void load(components::python_t&);
    private:

        auto jupyter_kernel_init(zmq::context_t& , boost::uuids::uuid , std::function<void(const std::string&, std::vector<std::string>)> ) -> void;

        auto jupyter_engine_init(boost::uuids::uuid , std::function<void(const std::string&, std::vector<std::string>)> ) -> void;

        components::sandbox_mode_t mode_;
        components::log_t log_;
        boost::filesystem::path jupyter_connection_path_;
        std::unique_ptr<components::python_t> python_interpreter_;
        std::unique_ptr<zmq::socket_t> stdin_socket_;
        boost::intrusive_ptr<interactive_python_interpreter::jupyter::pykernel> jupyter_kernel_;
    };
} // namespace services
