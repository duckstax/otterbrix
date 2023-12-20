#include <catch2/catch.hpp>

#include <climits>

#include <components/document/core/dict.hpp>
#include <components/document/core/shared_keys.hpp>

using namespace document::impl;
using document::retained_t;

TEST_CASE("shared_keys_t") {
    SECTION("empty") {
        retained_t<shared_keys_t> sk = new shared_keys_t();
        REQUIRE(sk->count() == 0);
    }

    SECTION("eligibility") {
        retained_t<shared_keys_t> sk = new shared_keys_t();
        int key = 100;
        REQUIRE(sk->encode_and_add("", key));
        REQUIRE(sk->encode_and_add("x", key));
        REQUIRE(sk->encode_and_add("aZ_019-", key));
        REQUIRE(sk->encode_and_add("abcdefghijklmnop", key));
        REQUIRE(sk->encode_and_add("-", key));
        REQUIRE_FALSE(sk->encode_and_add("@", key));
        REQUIRE_FALSE(sk->encode_and_add("abc.jpg", key));
        REQUIRE_FALSE(sk->encode_and_add("abcdefghijklmnopq", key));
        REQUIRE_FALSE(sk->encode_and_add("two words", key));
        REQUIRE_FALSE(sk->encode_and_add("aççents", key));
        REQUIRE_FALSE(sk->encode_and_add("☠️", key));
    }

    SECTION("encode") {
        retained_t<shared_keys_t> sk = new shared_keys_t();
        int key;
        REQUIRE(sk->encode_and_add("zero", key));
        REQUIRE(key == 0);
        REQUIRE(sk->count() == 1);
        REQUIRE(sk->encode_and_add("one", key));
        REQUIRE(key == 1);
        REQUIRE(sk->count() == 2);
        REQUIRE(sk->encode_and_add("two", key));
        REQUIRE(key == 2);
        REQUIRE(sk->count() == 3);
        REQUIRE_FALSE(sk->encode_and_add("@", key));
        REQUIRE(sk->count() == 3);
        REQUIRE(sk->encode_and_add("three", key));
        REQUIRE(key == 3);
        REQUIRE(sk->count() == 4);
        REQUIRE(sk->encode_and_add("four", key));
        REQUIRE(key == 4);
        REQUIRE(sk->count() == 5);
        REQUIRE(sk->encode_and_add("two", key));
        REQUIRE(key == 2);
        REQUIRE(sk->count() == 5);
        REQUIRE(sk->encode_and_add("zero", key));
        REQUIRE(key == 0);
        REQUIRE(sk->count() == 5);
        REQUIRE(sk->by_key() == (std::vector<std::string>{"zero", "one", "two", "three", "four"}));
    }

    SECTION("decode") {
        retained_t<shared_keys_t> sk = new shared_keys_t();
        int key;
        REQUIRE(sk->encode_and_add("zero", key));
        REQUIRE(sk->encode_and_add("one", key));
        REQUIRE(sk->encode_and_add("two", key));
        REQUIRE(sk->encode_and_add("three", key));
        REQUIRE(sk->encode_and_add("four", key));

        REQUIRE(sk->decode(2) == "two");
        REQUIRE(sk->decode(0) == "zero");
        REQUIRE(sk->decode(3) == "three");
        REQUIRE(sk->decode(1) == "one");
        REQUIRE(sk->decode(4) == "four");

        REQUIRE_FALSE(!sk->decode(5).empty());
        REQUIRE_FALSE(!sk->decode(2047).empty());
        REQUIRE_FALSE(!sk->decode(INT_MAX).empty());
    }

    SECTION("revert_to_count") {
        retained_t<shared_keys_t> sk = new shared_keys_t();
        int key;
        REQUIRE(sk->encode_and_add("zero", key));
        REQUIRE(sk->encode_and_add("one", key));
        REQUIRE(sk->encode_and_add("two", key));
        REQUIRE(sk->encode_and_add("three", key));
        REQUIRE(sk->encode_and_add("four", key));

        sk->revert_to_count(3);

        REQUIRE(sk->count() == 3);
        REQUIRE_FALSE(!sk->decode(3).empty());
        REQUIRE_FALSE(!sk->decode(4).empty());
        REQUIRE(sk->by_key() == std::vector<std::string>{"zero", "one", "two"});
        REQUIRE(sk->encode_and_add("zero", key));
        REQUIRE(key == 0);
        REQUIRE(sk->encode_and_add("three", key));
        REQUIRE(key == 3);

        sk->revert_to_count(3);
        REQUIRE(sk->count() == 3);
        REQUIRE(sk->by_key() == std::vector<std::string>{"zero", "one", "two"});

        sk->revert_to_count(0);
        REQUIRE(sk->count() == 0);
        REQUIRE(sk->by_key() == std::vector<std::string>{});
        REQUIRE(sk->encode_and_add("three", key));
        REQUIRE(sk->count() == 1);
        REQUIRE(key == 0);
    }

    SECTION("many keys") {
        retained_t<shared_keys_t> sk = new shared_keys_t();
        for (size_t i = 0; i < shared_keys_t::max_count; i++) {
            REQUIRE(sk->count() == i);
            char str[10];
            sprintf(str, "K%zu", i);
            int key;
            sk->encode_and_add(std::string(str), key);
            REQUIRE(static_cast<size_t>(key) == i);
        }

        int key;
        REQUIRE_FALSE(sk->encode_and_add("unnecessary", key));

        for (size_t i = 0; i < shared_keys_t::max_count; i++) {
            char str[10];
            sprintf(str, "K%zu", i);
            REQUIRE(sk->decode(static_cast<int>(i)) == std::string(str));
        }
    }
}
