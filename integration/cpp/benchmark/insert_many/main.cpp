#include "../classes.hpp"

void insert_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    dispatcher->create_database(session, database_name);
    dispatcher->create_collection(session, database_name, collection_name);
    state.ResumeTiming();
    for (auto _ : state) {
        std::pmr::vector<document_ptr> docs;
        for (int i = 0; i < state.range(0); ++i) {
            docs.push_back(gen_doc(int(10000 * state.range(0) + i), dispatcher->resource()));
        }
        dispatcher->insert_many(session, database_name, collection_name, docs);
    }
}
BENCHMARK(insert_many)->Arg(1)->Arg(10)->Arg(20)->Arg(100)->Arg(500)->Arg(1000);

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
