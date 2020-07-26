#include "init_service.hpp"

#include <goblin-engineer/components/root_manager.hpp>
#include <services/interactive_python_interpreter/interactive_python.hpp>
#include <services/jupyter/jupyter.hpp>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;
using namespace goblin_engineer::components;

void init_service(goblin_engineer::components::root_manager& env, components::configuration& cfg, components::log_t& log) {
    auto jupyter = make_manager_service<services::jupyter>(env, cfg.python_configuration_, log);
    make_service<services::interactive_python>(jupyter,cfg.python_configuration_, log);

}
