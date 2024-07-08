#include <benchmark/benchmark.h>
#include <memory_resource>
#include "../document/document.hpp"
#include "../components/tests/generaty.hpp"

using components::document::document_t;

void read_wrong(benchmark::State &state) {
    auto allocator = std::pmr::unsynchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    std::string_view key_bool{"/countBool"};

    auto f = [&doc, key_bool]() {
        doc->is_long(key_bool);
        doc->is_ulong(key_bool);
        doc->is_double(key_bool);

        doc->get_ulong(key_bool);
        doc->get_long(key_bool);
        doc->get_double(key_bool);
        doc->get_string(key_bool);
        doc->get_array(key_bool);
        doc->get_dict(key_bool);
    };

    for (auto _: state) {
        for (int i = 0; i < state.range(0); ++i) {
            f();
        }
    }
}
BENCHMARK(read_wrong)->Arg(100000);

void read(benchmark::State &state) {
    auto allocator = std::pmr::unsynchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    std::string_view key_int{"/count"};
    std::string_view key_str{"/countStr"};
    std::string_view key_double{"/countDouble"};
    std::string_view key_bool{"/countBool"};
    std::string_view key_array{"/countArray"};
    std::string_view key_dict{"/countDict"};

    auto f = [
                 &doc,
                 key_int,
                 key_str,
                 key_double,
                 key_bool,
                 key_array,
                 key_dict
    ]() {
        doc->is_exists(key_int);
        doc->is_long(key_int);
        doc->is_ulong(key_int);
        doc->is_double(key_double);

        doc->get_bool(key_bool);
        doc->get_long(key_int);
        doc->get_ulong(key_int);
        doc->get_double(key_double);
        doc->get_string(key_str);
        doc->get_array(key_array);
        doc->get_dict(key_dict);
    };

    for (auto _: state) {
        for (int i = 0; i < state.range(0); ++i) {
            f();
        }
    }
}
BENCHMARK(read)->Arg(100000);

void deep_read(benchmark::State &state) {
    auto allocator = std::pmr::unsynchronized_pool_resource();
    auto doc = gen_doc(1000, &allocator);
    std::string_view array_int_key{"/countArray/3"};
    std::string_view array_array_key{"/nestedArray/2"};
    std::string_view dict_dict_key{"/mixedDict/1001"};
    std::string_view array_dict_key{"/dictArray/3"};
    std::string_view array_array_int_key{"/nestedArray/2/2"};
    std::string_view array_dict_int_key{"/dictArray/3/number"};
    std::string_view dict_dict_bool_key{"/mixedDict/1001/odd"};

    auto f = [
                 &doc,
                 array_int_key,
                 array_array_key,
                 dict_dict_key,
                 array_dict_key,
                 array_array_int_key,
                 array_dict_int_key,
                 dict_dict_bool_key
    ]() {
        doc->is_exists(array_int_key);
        doc->is_int(array_int_key);
        doc->is_long(array_int_key);

        doc->get_bool(dict_dict_bool_key);
        doc->get_long(array_int_key);
        doc->get_long(array_array_int_key);
        doc->get_long(array_dict_int_key);
        doc->get_array(array_array_key);
        doc->get_dict(dict_dict_key);
        doc->get_dict(array_dict_key);
    };

    for (auto _: state) {
        for (int i = 0; i < state.range(0); ++i) {
            f();
        }
    }
}
BENCHMARK(deep_read)->Arg(100000);

BENCHMARK_MAIN();