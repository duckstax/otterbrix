#include "../classes.hpp"

void update_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = ottergon::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            dispatcher->update_many(session, database_name, collection_name, make_condition("count", "$lt", 100 * i), make_condition("$set", "count", 0), false);
            dispatcher->update_many(session, database_name, collection_name, make_condition("count", "$gt", size_collection - 100 * i), make_condition("$set", "count", size_collection), false);
        }
        dispatcher->update_many(session, database_name, collection_name, make_document(), make_condition("$set", "count", 0), false);
    }
}
BENCHMARK(update_many)->Arg(100);


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
