#pragma once

#include <goblin-engineer/components/root_manager.hpp>
#include <rocketjoe/configuration/configuration.hpp>
#include <rocketjoe/log/log.hpp>

void init_service(goblin_engineer::components::root_manager&, rocketjoe::configuration&, rocketjoe::log_t&);