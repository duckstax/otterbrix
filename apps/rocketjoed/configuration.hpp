#pragma once

#include <cxxopts.hpp>

#include <goblin-engineer.hpp>

void logo();

void load_or_generate_config(cxxopts::ParseResult& result, goblin_engineer::dynamic_config& configuration);
