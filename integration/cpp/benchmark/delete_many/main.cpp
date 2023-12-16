#include "../classes.hpp"

void delete_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            dispatcher->delete_many(session, database_name, collection_name, make_condition("count", "$lt", 100 * i));
            dispatcher->delete_many(session,
                                    database_name,
                                    collection_name,
                                    make_condition("count", "$gt", size_collection - 100 * i));
        }
        dispatcher->delete_many(session, database_name, collection_name, make_document());
    }
}
BENCHMARK(delete_many)->Arg(100);

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
