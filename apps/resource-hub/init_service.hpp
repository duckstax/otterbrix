#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <zmq.hpp>

void init_service(goblin_engineer::components::root_manager&, components::configuration&, components::log_t&,zmq::context_t&);
