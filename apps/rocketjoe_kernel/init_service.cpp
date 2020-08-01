#include "init_service.hpp"

#include <services/interactive_python_interpreter/interactive_python.hpp>
#include <services/jupyter/jupyter.hpp>

void init_service(actor_zeta::intrusive_ptr<services::jupyter> env, components::configuration& cfg, components::log_t& log) {
    log.info("void init_service(");
    auto python = services::make_service<services::interactive_python>(env,cfg.python_configuration_, log);
    if(python){
        log.info("if(python){");
    }
    log.info("void init_service(");
}
