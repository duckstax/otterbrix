#include <catch2/catch.hpp>
#include "storage.hpp"
#include "temp_array.hpp"
#include "slice_io.hpp"
#include "bitmap.hpp"
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
    for (int i = 0; i < size; ++i) {
        ++hist[bucket[i]];
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


TEST_CASE("concurrent_map_t") {
    SECTION("basic") {
        concurrent_map_t map(2048);
        REQUIRE(map.count() == 0);
        REQUIRE(map.capacity() >= 2048);
        REQUIRE(map.string_bytes_count() == 0);
        REQUIRE(map.string_bytes_capacity() >= 2048 * 16);

        REQUIRE(map.find("cat").key == nullptr);
        auto cat = map.insert("cat", 0x1298);
        REQUIRE(cat.key == "cat");
        REQUIRE(cat.value == 0x1298);

        auto found = map.find("cat");
        REQUIRE(found.key == cat.key);
        REQUIRE(found.value == cat.value);

        found = map.insert("cat", 0xdead);
        REQUIRE(found.key == cat.key);
        REQUIRE(found.value == cat.value);

        found = map.find("dog");
        REQUIRE_FALSE(found.key);
        REQUIRE_FALSE(map.remove("dog"));

        for (int pass = 1; pass <= 2; ++pass) {
            for (uint16_t i = 0; i < 2000; ++i) {
                char keybuf[10];
                sprintf(keybuf, "k-%04d", i);
                auto result = (pass == 1) ? map.insert(keybuf, i) : map.find(keybuf);
                REQUIRE(result.key == keybuf);
                REQUIRE(result.value == i);
            }
        }

        REQUIRE(map.remove("cat"));
        found = map.find("cat");
        REQUIRE_FALSE(found.key);
        REQUIRE(map.count() == 2000);
        REQUIRE(map.string_bytes_count() > 0);
    }

    SECTION("concurrency") {
        constexpr int size = 6000;
        concurrent_map_t map(size);
        REQUIRE(map.count() == 0);
        REQUIRE(map.capacity() >= size);
        REQUIRE(map.string_bytes_capacity() >= 65535);

        std::vector<std::string> keys;
        for (int i = 0; i < size; ++i) {
            char keybuf[10];
            sprintf(keybuf, "%x", i);
            keys.push_back(keybuf);
        }

        auto reader = [&](unsigned step) {
            unsigned index = static_cast<unsigned>(random()) % size;
            for (int n = 0; n < 2 * size; ++n) {
                auto e = map.find(keys[index].c_str());
                if (e.key) {
                    REQUIRE(e.key == keys[index].c_str());
                    REQUIRE(e.value == index);
                }
                index = (index + step) % size;
            }
        };

        auto writer = [&](unsigned step, bool delete_too) {
            unsigned const start_index = static_cast<unsigned>(random()) % size;
            auto index = start_index;
            for (int n = 0; n < size; n++) {
                auto value = static_cast<uint16_t>(index & 0xFFFF);
                auto e = map.insert(keys[index].c_str(), value);
                if (e.key == null_slice) {
                    throw std::runtime_error("concurrent_map_t overflow");
                }
                REQUIRE(e.key == keys[index].c_str());
                REQUIRE(e.value == value);
                index = (index + step) % size;
            }
            if (delete_too) {
                index = start_index;
                for (int n = 0; n < size; n++) {
                    map.remove(keys[index].c_str());
                    index = (index + step) % size;
                }
            }
        };

        auto f1 = std::async(reader, 7);
        auto f2 = std::async(reader, 53);
        auto f3 = std::async(writer, 23, true);
        auto f4 = std::async(writer, 91, true);

        f1.wait();
        f2.wait();
        f3.wait();
        f4.wait();

        REQUIRE(map.count() == 0);
    }
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
    for (auto str : strings) {
        auto encode_str = writer_t::encode_base64(static_cast<slice_t>(str));
        REQUIRE(encode_str == static_cast<slice_t>(encode_strings[i++]));
        auto decode_str = writer_t::decode_base64(static_cast<slice_t>(encode_str));
        REQUIRE(decode_str == str);
        encode_str.release();
        decode_str.release();
    }
}
