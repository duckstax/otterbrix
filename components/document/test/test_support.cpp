#include <catch2/catch.hpp>
#include <components/document/support/temp_array.hpp>
#include <components/document/support/slice_io.hpp>

using namespace document;

template <class T>
static void test_temp_array(size_t n, bool on_heap) {
    int64_t before = -1;
    _temp_array(array, T, n);
    int64_t after = -1;

    REQUIRE(sizeof(array[0]) == sizeof(T));
    REQUIRE(array.on_heap == on_heap);
    for (size_t i = 0; i < n; ++i) {
        array[i] = 0;
    }
    REQUIRE(before == -1);
    REQUIRE(after == -1);
}

TEST_CASE("temp_array_t") {
    test_temp_array<uint8_t>(0, false);
    test_temp_array<uint8_t>(1, false);
    test_temp_array<uint8_t>(1023, false);
    test_temp_array<uint8_t>(1024, true);
    for (size_t n = 1; n < 1000000; n *= 7) {
        test_temp_array<uint8_t>(n, n >= 1024);
    }
    for (size_t n = 1; n < 1000000; n *= 7) {
        test_temp_array<uint64_t>(n, n >= 1024/8);
    }
}


TEST_CASE("slice_t") {
    const char *file_path = "test_slice";
    slice_t data = "Data to write to a file.";
    write_to_file(data, file_path);
    alloc_slice_t read_data = read_file(file_path);
    REQUIRE(read_data == data);
    append_to_file(" Append new data.", file_path);
    read_data = read_file(file_path);
    REQUIRE(read_data == "Data to write to a file. Append new data.");
}


TEST_CASE("hash") {
    constexpr int size = 4096;
    constexpr int count_keys = 2048;
    int bucket[size] = {0};
    for (int i = 0; i < count_keys; ++i) {
        char keybuf[10];
        sprintf(keybuf, "k-%04d", i);
        auto hash = slice_t(keybuf).hash();
        auto index = hash & (size-1);
        ++bucket[index];
    }
    int hist[size] = {0};
    for (int i : bucket) {
        ++hist[i];
    }
    int total = 0;
    for (int i = size - 1; i >= 0; --i) {
        if (hist[i] > 0 || total > 0) {
            total += i * hist[i];
            REQUIRE(i <= 7);
        }
    }
    REQUIRE(total == count_keys);
}
