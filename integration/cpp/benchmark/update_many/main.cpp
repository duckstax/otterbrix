#include "../classes.hpp"

void update_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto p = create_aggregate(database_name, collection_name, compare_type::lt, "count", 100 * i);
            dispatcher->update_many(session,
                                    p.first,
                                    p.second,
                                    document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(0) + "}}",
                                                                   dispatcher->resource()),
                                    false);
            p = create_aggregate(database_name, collection_name, compare_type::gt, "count", size_collection - 100 * i);
            dispatcher->update_many(
                session,
                p.first,
                p.second,
                document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(size_collection) + "}}",
                                               dispatcher->resource()),
                false);
        }
        auto p = create_aggregate(database_name, collection_name, compare_type::all_true);
        dispatcher->update_many(session,
                                p.first,
                                p.second,
                                document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(0) + "}}",
                                                               dispatcher->resource()),
                                false);
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
