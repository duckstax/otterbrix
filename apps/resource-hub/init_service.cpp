#include "init_service.hpp"

#include <components/python_sandbox/python_sandbox.hpp>

#include <goblin-engineer/components/http.hpp>
#include <goblin-engineer/components/root_manager.hpp>
#include <iostream>

using goblin_engineer::components::make_manager_service;
using goblin_engineer::components::make_service;
using goblin_engineer::components::root_manager;
using namespace goblin_engineer::components;

using actor_zeta::link;

constexpr const static bool master = true;

void init_service(goblin_engineer::components::root_manager& env, nlohmann::json& cfg) {

}
