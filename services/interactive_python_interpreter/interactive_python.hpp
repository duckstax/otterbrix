#pragma once

#include <boost/uuid/uuid.hpp>

#include <components/buffer/zmq_buffer.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/python_sandbox/python_sandbox.hpp>

#include <goblin-engineer.hpp>
#include <goblin-engineer/abstract_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

namespace services {

    class jupyter;

    class interactive_python final : public goblin_engineer::abstract_service {
    public:
        interactive_python(
            actor_zeta::intrusive_ptr<jupyter>,
            const components::python_sandbox_configuration&,
            components::log_t&);

        void async_init(zmq::context_t&, boost::uuids::uuid identifier, std::function<void(const std::string&, std::vector<std::string>)>);

    private:
        auto registration(std::vector<std::string>) -> void;

        auto dispatch_shell(components::zmq_buffer_t&) -> void;

        auto dispatch_control(components::zmq_buffer_t&) -> void;

        auto stop_session() -> void;

        auto start_session() -> void;

        components::log_t log_;
        std::unique_ptr<components::python_interpreter> python_interpreter_;
    };
} // namespace services
