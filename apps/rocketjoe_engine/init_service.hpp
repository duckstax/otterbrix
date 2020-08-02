#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <services/jupyter/jupyter.hpp>

void init_service(actor_zeta::intrusive_ptr<services::jupyter>, components::configuration& , components::log_t& );
