#include <catch2/catch.hpp>
#include "temp_array.hpp"
#include "slice_io.hpp"
#include "bitmap.hpp"
#include "small_vector.hpp"
#include "writer.hpp"
#include <future>

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


TEST_CASE("bitmap_t") {
    REQUIRE(popcount(0) == 0);
    REQUIRE(popcount(0l) == 0);
    REQUIRE(popcount(0ll) == 0);
    REQUIRE(popcount(-1) == sizeof(int)*8);
    REQUIRE(popcount(-1l) == sizeof(long)*8);
    REQUIRE(popcount(-1ll) == sizeof(long long)*8);

    bitmap_t<uint32_t> b(0x12345678);
    REQUIRE(bitmap_t<uint32_t>::capacity == 32);
    REQUIRE(!b.empty());
    REQUIRE(b.bit_count() == 13);
    REQUIRE(b.index_of_bit(8) == 4);
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


TEST_CASE("small_vector_t") {
    SECTION("small") {
        small_vector_t<alloc_slice_t, 2> moved_strings;
        small_vector_t<alloc_slice_t, 2> strings;
        strings.emplace_back("string 1");
        strings.emplace_back("string 2");
        REQUIRE(strings.size() == 2);
        REQUIRE(strings[0] == "string 1");
        REQUIRE(strings[1] == "string 2");

        auto moved_strings_constructor(std::move(strings));
        REQUIRE(moved_strings_constructor.size() == 2);
        REQUIRE(moved_strings_constructor[0] == "string 1");
        REQUIRE(moved_strings_constructor[1] == "string 2");

        moved_strings = std::move(moved_strings_constructor);
        REQUIRE(moved_strings.size() == 2);
        REQUIRE(moved_strings[0] == "string 1");
        REQUIRE(moved_strings[1] == "string 2");
    }

    SECTION("big") {
        small_vector_t<alloc_slice_t, 2> moved_strings;
        small_vector_t<alloc_slice_t, 2> strings;
        strings.emplace_back("string 1");
        strings.emplace_back("string 2");
        strings.emplace_back("string 3");
        REQUIRE(strings.size() == 3);
        REQUIRE(strings[0] == "string 1");
        REQUIRE(strings[1] == "string 2");
        REQUIRE(strings[2] == "string 3");

        auto moved_strings_constructor(std::move(strings));
        REQUIRE(moved_strings_constructor.size() == 3);
        REQUIRE(moved_strings_constructor[0] == "string 1");
        REQUIRE(moved_strings_constructor[1] == "string 2");
        REQUIRE(moved_strings_constructor[2] == "string 3");

        moved_strings = std::move(moved_strings_constructor);
        REQUIRE(moved_strings.size() == 3);
        REQUIRE(moved_strings[0] == "string 1");
        REQUIRE(moved_strings[1] == "string 2");
        REQUIRE(moved_strings[2] == "string 3");
    }
}


TEST_CASE("Base64 encode and decode") {
    std::vector<std::string> strings = {"a", "ab", "abc", "abcd", "abcde"};
    std::vector<std::string> encode_strings = {"YQ==", "YWI=", "YWJj", "YWJjZA==", "YWJjZGU="};
    size_t i = 0;
    for (const auto& str : strings) {
        auto encode_str = writer_t::encode_base64(static_cast<slice_t>(str));
        REQUIRE(encode_str == static_cast<slice_t>(encode_strings[i++]));
        auto decode_str = writer_t::decode_base64(static_cast<slice_t>(encode_str));
        REQUIRE(decode_str == str);
        encode_str.release();
        decode_str.release();
    }
}
