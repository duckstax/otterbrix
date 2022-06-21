#include "../classes.hpp"
#include <components/tests/generaty.hpp>


/// insert_one
void insert_one(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = duck_charmer::session_id_t();
    dispatcher->create_database(session, database_name);
    auto collection = collection_name + "_insert_one";
    dispatcher->create_collection(session, database_name, collection);
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto doc = gen_doc(int(10000 * state.range(0) + i));
            dispatcher->insert_one(session, database_name, collection, doc);
        }
    }
}
BENCHMARK(insert_one)->Arg(1)->Arg(10)->Arg(20)->Arg(100)->Arg(500)->Arg(1000);


/// insert_many
void insert_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = duck_charmer::session_id_t();
    dispatcher->create_database(session, database_name);
    auto collection = collection_name + "_insert_one";
    dispatcher->create_collection(session, database_name, collection);
    state.ResumeTiming();
    for (auto _ : state) {
        std::list<document_ptr> docs;
        for (int i = 0; i < state.range(0); ++i) {
            docs.push_back(gen_doc(int(10000 * state.range(0) + i)));
        }
        dispatcher->insert_many(session, database_name, collection, docs);
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
