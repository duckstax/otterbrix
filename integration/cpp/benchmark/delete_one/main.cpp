#include "../classes.hpp"

void delete_one(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            dispatcher->delete_one(session,
                                   database_name,
                                   collection_name,
                                   make_condition("id_", "$eq", std::to_string(i)));
            dispatcher->delete_one(session,
                                   database_name,
                                   collection_name,
                                   make_condition("id_", "$eq", std::to_string(size_collection - i)));
        }
    }
}
BENCHMARK(delete_one)->Arg(100);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    init_collection();
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
