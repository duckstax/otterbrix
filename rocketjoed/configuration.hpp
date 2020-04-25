#pragma once

#include <cxxopts.hpp>

#include <goblin-engineer.hpp>

#include <nlohmann/json.hpp>

auto logo() -> const char * ;

void load_or_generate_config(cxxopts::ParseResult& /*result*/, nlohmann::json& /*configuration*/);
