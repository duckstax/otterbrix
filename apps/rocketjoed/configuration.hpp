#pragma once

#include <cxxopts.hpp>

#include <goblin-engineer/dynamic_environment.hpp>

void logo();

void load_or_generate_config(cxxopts::ParseResult& result, goblin_engineer::configuration& configuration);
