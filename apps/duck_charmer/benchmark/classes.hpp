#pragma once

#include <apps/duck_charmer/spaces.hpp>
#include <benchmark/benchmark.h>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";


inline configuration::config create_null_config() {
    auto config = configuration::config::default_config();
    config.log.level = log_t::level::off;
    config.disk.on = false;
    config.wal.sync_to_disk = false;
    return config;
}

class unique_spaces final : public duck_charmer::base_spaces {
public:
    static unique_spaces &get() {
        static unique_spaces spaces_;
        return spaces_;
    }

private:
    unique_spaces()
        : duck_charmer::base_spaces(create_null_config())
    {}
};
