#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/python_sandbox/python_sandbox.hpp>

#include <goblin-engineer/abstract_manager_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

namespace services {
    class interactive_python_interpreter final : public goblin_engineer::abstract_manager_service {
    public:
        interactive_python_interpreter(
            goblin_engineer::components::root_manager*
            , const components::python_sandbox_configuration&
            , components::log_t&);

        void enqueue(goblin_engineer::message, actor_zeta::execution_device*) override;

        auto start() -> void;

        auto init() -> void;

    private:
        components::log_t log_;
        components::python_interpreter python_interpreter_;
    };
} // namespace services