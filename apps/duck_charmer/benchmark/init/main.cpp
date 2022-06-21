#include "../classes.hpp"


/// create databases
void create_databases(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_database(session, database_name + "_" + std::to_string(state.range(0)) + "_" + std::to_string(i));
        }
    }
}
BENCHMARK(create_databases)->Arg(1)->Arg(10)->Arg(20)->Arg(100);


/// create collections
void create_collections(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = duck_charmer::session_id_t();
    dispatcher->create_database(session, database_name);
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            session = duck_charmer::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name + "_" + std::to_string(state.range(0)) + "_" + std::to_string(i));
        }
    }
}
BENCHMARK(create_collections)->Arg(1)->Arg(10)->Arg(20)->Arg(100);


int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    unique_spaces::get();
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
