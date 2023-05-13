#include <benchmark/benchmark.h>
#include <components/document/document_view.hpp>
#include <components/tests/generaty.hpp>

using namespace components::document;

void document_read(benchmark::State& state) {
    state.PauseTiming();
    auto doc = gen_doc(1000);
    document_view_t view{doc};
    std::string key_int{"count"};
    std::string key_str{"countStr"};
    std::string key_double{"countDouble"};
    std::string key_bool{"countBool"};
    std::string key_array{"countArray"};
    std::string key_dict{"countDict"};

    auto f = [&view](const std::string &key) {
        view.is_exists(key);
        view.is_null(key);
        view.is_bool(key);
        view.is_ulong(key);
        view.is_long(key);
        view.is_double(key);
        view.is_string(key);
        view.is_array(key);
        view.is_dict(key);

        view.get(key);
        view.get_bool(key);
        view.get_ulong(key);
        view.get_long(key);
        view.get_double(key);
        view.get_string(key);
        view.get_array(key);
        view.get_dict(key);
    };

    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            f(key_int);
            f(key_str);
            f(key_double);
            f(key_bool);
            f(key_array);
            f(key_dict);
        }
    }
}
BENCHMARK(document_read)->Arg(10000);


int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
