#include <catch2/catch.hpp>
#include <components/document/core/slice.hpp>

using namespace document;

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
