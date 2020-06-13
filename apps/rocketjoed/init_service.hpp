#pragma once

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <goblin-engineer/components/root_manager.hpp>

void init_service(goblin_engineer::components::root_manager& env, rocketjoe::configuration& cfg, components::log_t& log);
