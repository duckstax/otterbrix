#pragma once

#include <cxxopts.hpp>

#include <goblin-engineer/dynamic_environment.hpp>

#include <yaml-cpp/yaml.h>

void convector(YAML::Node &input, goblin_engineer::dynamic_config &output);

void logo();

void load_config( cxxopts::ParseResult& args_, goblin_engineer::configuration & config );

void generate_yaml_config( YAML::Node& config_ );

void generate_config( goblin_engineer::configuration & config );
