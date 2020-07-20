#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/python_sandbox/python_sandbox.hpp>

#include <goblin-engineer.hpp>
#include <goblin-engineer/abstract_service.hpp>
#include <goblin-engineer/components/root_manager.hpp>

namespace services {

    class interactive_python_interpreter final : public goblin_engineer::abstract_service {
    public:
        interactive_python_interpreter(
            goblin_engineer::components::root_manager*
            , const components::python_sandbox_configuration&
            , components::log_t&);

    private:
        auto registration(std::vector<std::string>) -> void;

        auto dispatch_shell(std::vector<std::string> msgs) -> void;

        auto dispatch_control(std::vector<std::string> msgs) -> void;

        components::log_t log_;
        std::unique_ptr<components::python_interpreter> python_interpreter_;
    };
} // namespace services