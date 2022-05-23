#pragma once

#include <apps/duck_charmer/spaces.hpp>

inline components::config test_create_config(const boost::filesystem::path &path) {
    auto config = components::config::default_config();
    config.log.path = path;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

inline void test_clear_directory(const components::config &config) {
    boost::filesystem::remove_all(config.disk.path);
    boost::filesystem::create_directories(config.disk.path);
}

class test_spaces final : public duck_charmer::base_spaces {
public:
    test_spaces(const components::config &config)
        : duck_charmer::base_spaces(config)
    {}
};
