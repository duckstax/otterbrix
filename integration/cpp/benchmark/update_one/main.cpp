#include "../classes.hpp"

void update_one(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto p = create_aggregate(database_name, collection_name, compare_type::eq, "_id", std::to_string(i));
            dispatcher->update_one(session,
                                   p.first,
                                   p.second,
                                   document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(0) + "}}",
                                                                  dispatcher->resource()),
                                   false);
            p = create_aggregate(database_name,
                                 collection_name,
                                 compare_type::eq,
                                 "_id",
                                 std::to_string(size_collection - i));
            dispatcher->update_one(
                session,
                p.first,
                p.second,
                document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(size_collection) + "}}",
                                               dispatcher->resource()),
                false);
        }
    }
}
BENCHMARK(update_one)->Arg(100);

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
