#pragma once

#include <apps/duck_charmer/spaces.hpp>
#include <benchmark/benchmark.h>

inline configuration::config create_null_config() {
    auto config = configuration::config::default_config();
    config.log.level = log_t::level::off;
    config.disk.on = false;
    config.wal.sync_to_disk = false;
    return config;
}

class test_spaces final : public duck_charmer::base_spaces {
public:
    test_spaces()
        : duck_charmer::base_spaces(create_null_config())
    {}
};

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


/// initialization
void init_space(benchmark::State& state) {
    for (auto _ : state) {
        test_spaces space;
    }
}
BENCHMARK(init_space);


/// create databases
void create_databases(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        auto* dispatcher = unique_spaces::get().dispatcher();
        state.ResumeTiming();
        for (int i = 0; i < state.range(0); ++i) {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_database(session, "database_" + std::to_string(state.range(0)) + "_" + std::to_string(i));
        }
    }
}
BENCHMARK(create_databases)->Arg(1);//->Arg(10)->Arg(20)->Arg(100)->Arg(1000);

//benchmark::DoNotOptimize();
