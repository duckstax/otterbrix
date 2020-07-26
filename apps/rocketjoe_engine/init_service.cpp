#include "init_service.hpp"

#include <goblin-engineer/components/root_manager.hpp>
#include <services/interactive_python_interpreter/interactive_python.hpp>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;
using namespace goblin_engineer::components;

void init_service(goblin_engineer::components::root_manager& env, components::configuration& cfg, components::log_t& log) {
    if (cfg.python_configuration_.mode_ != components::sandbox_mode_t::script) {
        auto python = make_manager_service<services::interactive_python>(env, cfg.python_configuration_, log);
        python->init();
        python->start();
    }

}
