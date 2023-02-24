#include <benchmark/benchmark.h>
#include "../classes.hpp"

//#define test_with_disk

using components::expressions::compare_type;

constexpr bool disk_on = true;
constexpr bool disk_off = false;
constexpr bool index_on = true;
constexpr bool index_off = false;

#define BENCHMARK_FUNC(FUNC, DISK_ON) \
    BENCHMARK(FUNC<DISK_ON, index_off>)->Arg(100); \
    BENCHMARK(FUNC<DISK_ON, index_on>)->Arg(100)

template <bool on_disk, bool on_index>
void only_find_all(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = wr_dispatcher<on_disk>();
    collection_name_t collection_name = get_collection_name<on_index>();
    auto session = duck_charmer::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            dispatcher->find(session, create_aggregate(collection_name));
        }
    }
}

template <bool on_disk, bool on_index>
void only_find_eq(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = wr_dispatcher<on_disk>();
    collection_name_t collection_name = get_collection_name<on_index>();
    auto session = duck_charmer::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            dispatcher->find(session, create_aggregate(collection_name, compare_type::eq, "count", 115));
        }
    }
}

template <bool on_disk, bool on_index>
void only_find_gt(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = wr_dispatcher<on_disk>();
    collection_name_t collection_name = get_collection_name<on_index>();
    auto session = duck_charmer::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            dispatcher->find(session, create_aggregate(collection_name, compare_type::gt, "count", size_collection - 100));
        }
    }
}



BENCHMARK_FUNC(only_find_all, disk_off);
BENCHMARK_FUNC(only_find_eq, disk_off);
BENCHMARK_FUNC(only_find_gt, disk_off);

#ifdef test_with_disk
BENCHMARK_FUNC(only_find_all, disk_on);
BENCHMARK_FUNC(only_find_eq, disk_on);
BENCHMARK_FUNC(only_find_gt, disk_on);
#endif

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    init_spaces<disk_off>();
#ifdef test_with_disk
    init_spaces<disk_on>();
#endif
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
