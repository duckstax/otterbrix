#include <catch2/catch.hpp>

#include <components/document/json/json_coder.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/support/slice_io.hpp>

#include "doc.hpp"

using namespace document;
using namespace document::impl;

TEST_CASE("mutable::mutable_array_t") {

    SECTION("check type") {
        auto ma = mutable_array_t::new_array();

        REQUIRE(ma->as_array() == ma);
        REQUIRE(ma->is_mutable());
        REQUIRE(ma->as_mutable() == ma);
        REQUIRE(ma->type() == value_type::array);
        REQUIRE_FALSE(ma->is_int());
        REQUIRE_FALSE(ma->is_unsigned());
        REQUIRE_FALSE(ma->is_double());
        REQUIRE(ma->as_bool() == true);
        REQUIRE(ma->as_int() == 0);
        REQUIRE(ma->as_unsigned() == 0);
        REQUIRE(ma->as_float() == 0.0);
        REQUIRE(ma->as_double() == 0.0);
        REQUIRE(ma->as_string() == null_slice);
        REQUIRE(ma->as_data() == null_slice);
        REQUIRE(ma->to_string() == null_slice);
        REQUIRE(ma->as_dict() == nullptr);
        REQUIRE(ma->as_array() == ma);
    }

    SECTION("set value") {
        constexpr uint32_t size = 17;
        auto ma = mutable_array_t::new_array();

        REQUIRE(ma->count() == 0);
        REQUIRE(ma->empty());
        REQUIRE_FALSE(ma->get(0));

        mutable_array_t::iterator_t i0(ma);
        REQUIRE_FALSE(i0);

        REQUIRE_FALSE(ma->is_changed());
        ma->resize(size);
        REQUIRE(ma->is_changed());
        REQUIRE(ma->count() == size);
        REQUIRE_FALSE(ma->empty());

        for (uint32_t i = 0; i < size; ++i) {
            REQUIRE(ma->get(i)->type() == value_type::null);
        }

        ma->set(0, null_value);
        ma->set(1, false);
        ma->set(2, true);
        ma->set(3, 0);
        ma->set(4, -123);
        ma->set(5, 2021);
        ma->set(6, 123456789);
        ma->set(7, -123456789);
        ma->set(8, slice_t("dog"));
        ma->set(9, static_cast<float>(M_PI));
        ma->set(10, M_PI);
        ma->set(11, std::numeric_limits<uint64_t>::max());
        ma->set(12, static_cast<int64_t>(0x100000000LL));
        ma->set(13, static_cast<uint64_t>(0x100000000ULL));
        ma->set(14, std::numeric_limits<int64_t>::min());
        ma->set(15, static_cast<int64_t>(9223372036854775807LL));
        ma->set(16, static_cast<int64_t>(-9223372036854775807LL));

        const value_type types[size] = {
            value_type::null, value_type::boolean, value_type::boolean, value_type::number, value_type::number, value_type::number,
            value_type::number, value_type::number, value_type::string, value_type::number, value_type::number, value_type::number,
            value_type::number, value_type::number, value_type::number, value_type::number, value_type::number
        };
        for (uint32_t i = 0; i < size; ++i) {
            REQUIRE(ma->get(i)->type() == types[i]);
        }
        REQUIRE(ma->get(1)->as_bool() == false);
        REQUIRE(ma->get(2)->as_bool() == true);
        REQUIRE(ma->get(3)->as_int() == 0);
        REQUIRE(ma->get(4)->as_int() == -123);
        REQUIRE(ma->get(5)->as_int() == 2021);
        REQUIRE(ma->get(6)->as_int() == 123456789);
        REQUIRE(ma->get(7)->as_int() == -123456789);
        REQUIRE(ma->get(8)->as_string() == slice_t("dog"));
        REQUIRE(ma->get(9)->as_float() == Approx(static_cast<float>(M_PI)));
        REQUIRE(ma->get(10)->as_double() == Approx(M_PI));
        REQUIRE(ma->get(11)->as_unsigned() == std::numeric_limits<uint64_t>::max());
        REQUIRE(ma->get(12)->as_int() == 0x100000000LL);
        REQUIRE(ma->get(13)->as_unsigned() == 0x100000000ULL);
        REQUIRE(ma->get(14)->as_int() == std::numeric_limits<int64_t>::min());
        REQUIRE(ma->get(15)->as_int() == 9223372036854775807LL);
        REQUIRE(ma->get(16)->as_int() == -9223372036854775807LL);

        mutable_array_t::iterator_t i1(ma);
        for (auto type : types) {
            REQUIRE(i1);
            REQUIRE(i1.value() != nullptr);
            REQUIRE(i1.value()->type() == type);
            ++i1;
        }
        REQUIRE_FALSE(i1);

        REQUIRE(ma->as_array()->to_json() == slice_t("[null,false,true,0,-123,2021,123456789,-123456789,\"dog\",3.141593,"
              "3.141592653589793,18446744073709551615,4294967296,4294967296,"
              "-9223372036854775808,9223372036854775807,-9223372036854775807]"));

        ma->remove(3, 5);
        REQUIRE(ma->count() == 12);
        REQUIRE(ma->get(2)->type() == value_type::boolean);
        REQUIRE(ma->get(2)->as_bool() == true);
        REQUIRE(ma->get(3)->type() == value_type::string);
        REQUIRE(ma->get(3)->as_string() == slice_t("dog"));

        ma->insert(1, 2);
        REQUIRE(ma->count() == 14);
        REQUIRE(ma->get(1)->type() == value_type::null);
        REQUIRE(ma->get(2)->type() == value_type::null);
        REQUIRE(ma->get(3)->type() == value_type::boolean);
        REQUIRE(ma->get(3)->as_bool() == false);
    }

    SECTION("to array_t") {
        auto ma = mutable_array_t::new_array();
        auto a = ma->as_array();
        REQUIRE(a->type() == value_type::array);
        REQUIRE(a->count() == 0);
        REQUIRE(a->empty());

        ma->resize(2);
        ma->set(0, 100);
        ma->set(1, 200);

        REQUIRE(a->count() == 2);
        REQUIRE_FALSE(a->empty());
        REQUIRE(a->get(0)->as_int() == 100);
        REQUIRE(a->get(1)->as_int() == 200);

        array_t::iterator i(a);
        REQUIRE(i);
        REQUIRE(i.value()->as_int() == 100);
        ++i;
        REQUIRE(i);
        REQUIRE(i.value()->as_int() == 200);
        ++i;
        REQUIRE_FALSE(i);
    }

    SECTION("copy") {
        auto ma = mutable_array_t::new_array(2);
        ma->set(0, 100);
        ma->set(1, slice_t("dog"));

        auto mb = mutable_array_t::new_array(1);
        mb->set(0, ma);
        REQUIRE(mb->get(0) == ma);

        auto mc = mutable_array_t::new_array(1);
        mc->set(0, mb);
        REQUIRE(mc->get(0) == mb);

        auto copy = mc->copy();
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE(copy->get(0) == mc->get(0));

        copy = mc->copy(deep_copy);
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE_FALSE(copy->get(0) == mc->get(0));
        REQUIRE_FALSE(copy->get(0)->as_array()->get(0) == ma);
    }

    SECTION("copy immutable") {
        auto doc = doc_t::from_json(slice_t("[100, \"dog\"]"));
        auto a = doc->root()->as_array();

        auto copy = mutable_array_t::new_array(a);
        REQUIRE(copy->source() == a);
        REQUIRE(copy->is_equal(a));

        auto mb = mutable_array_t::new_array(1);
        mb->set(0, a);
        REQUIRE(mb->get(0) == a);

        auto mc = mutable_array_t::new_array(1);
        mc->set(0, mb);
        REQUIRE(mc->get(0) == mb);

        copy = mc->copy();
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE(copy->get(0) == mc->get(0));

        copy = mc->copy(deep_copy);
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE_FALSE(copy->get(0) == mc->get(0));
        REQUIRE(copy->get(0)->as_array()->get(0) == a);

        copy = mc->copy(copy_flags(deep_copy_immutables));
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE_FALSE(copy->get(0) == mc->get(0));
        REQUIRE_FALSE(copy->get(0)->as_array()->get(0) == a);
    }

}


TEST_CASE("mutable::mutable_dict_t") {

    SECTION("check type") {
        auto md = mutable_dict_t::new_dict();
        REQUIRE(md->type() == value_type::dict);

        REQUIRE(md->is_mutable());
        REQUIRE(md->type() == value_type::dict);
        REQUIRE(!md->is_int());
        REQUIRE(!md->is_unsigned());
        REQUIRE(!md->is_double());

        REQUIRE(md->as_bool() == true);
        REQUIRE(md->as_int() == 0);
        REQUIRE(md->as_unsigned() == 0);
        REQUIRE(md->as_float() == 0.0);
        REQUIRE(md->as_double() == 0.0);
        REQUIRE(md->as_string() == null_slice);
        REQUIRE(md->as_data() == null_slice);
        REQUIRE(md->to_string() == null_slice);
        REQUIRE(md->as_array() == nullptr);
        REQUIRE(md->as_dict() == md);
        REQUIRE(md->as_mutable() == md);
    }

    SECTION("set value") {
        auto md = mutable_dict_t::new_dict();
        REQUIRE(md->count() == 0);
        REQUIRE(md->get("key") == nullptr);

        mutable_dict_t::iterator i0(md);
        REQUIRE_FALSE(i0);

        REQUIRE_FALSE(md->is_changed());
        md->set("null", null_value);
        md->set("f", false);
        md->set("t", true);
        md->set("z", 0);
        md->set("-", -123);
        md->set("+", 2021);
        md->set("hi", 123456789);
        md->set("lo", -123456789);
        md->set("str", slice_t("dog"));
        REQUIRE(md->is_changed());
        REQUIRE(md->count() == 9);

        static const slice_t keys[9] = {"+", "-", "f", "hi", "lo", "null", "str", "t", "z"};
        static const value_type types[9] = {
            value_type::number, value_type::number, value_type::boolean, value_type::number, value_type::number,
            value_type::null, value_type::string, value_type::boolean, value_type::number
        };
        for (int i = 0; i < 9; ++i) {
            REQUIRE(md->get(keys[i])->type() == types[i]);
        }

        REQUIRE(md->get("f")->as_bool() == false);
        REQUIRE(md->get("t")->as_bool() == true);
        REQUIRE(md->get("z")->as_int() == 0);
        REQUIRE(md->get("-")->as_int() == -123);
        REQUIRE(md->get("+")->as_int() == 2021);
        REQUIRE(md->get("hi")->as_int() == 123456789);
        REQUIRE(md->get("lo")->as_int() == -123456789);
        REQUIRE(md->get("str")->as_string() == slice_t("dog"));
        REQUIRE(md->get("foo") == nullptr);

        bool found[9] = { };
        mutable_dict_t::iterator i1(md);
        for (int i = 0; i < 9; ++i) {
            REQUIRE(i1);
            auto key = i1.key_string();
            auto j = std::find(&keys[0], &keys[9], key) - &keys[0];
            REQUIRE(j < 9);
            REQUIRE_FALSE(found[j]);
            found[j] = true;
            REQUIRE(i1.value());
            REQUIRE(i1.value()->type() == types[j]);
            ++i1;
        }
        REQUIRE_FALSE(i1);

        md->remove("lo");
        REQUIRE_FALSE(md->get("lo"));
        REQUIRE(md->count() == 8);

        md->remove_all();
        REQUIRE(md->count() == 0);
        mutable_dict_t::iterator i2(md);
        REQUIRE_FALSE(i2);
    }

    SECTION("to dict_t") {
        auto md = mutable_dict_t::new_dict();
        auto d = md->as_dict();
        REQUIRE(d->type() == value_type::dict);
        REQUIRE(d->count() == 0);
        REQUIRE(d->empty());
        REQUIRE_FALSE(d->get("key"));

        dict_t::iterator i0(d);
        REQUIRE_FALSE(i0);

        md->set("null", null_value);
        md->set("f", false);
        md->set("t", true);
        md->set("z", 0);
        md->set("-", -123);
        md->set("+", 2021);
        md->set("hi", 123456789);
        md->set("lo", -123456789);
        md->set("str", slice_t("dog"));

        static const slice_t keys[9] = {"+", "-", "f", "hi", "lo", "null", "str", "t", "z"};
        static const value_type types[9] = {
            value_type::number, value_type::number, value_type::boolean, value_type::number, value_type::number,
            value_type::null, value_type::string, value_type::boolean, value_type::number
        };
        for (int i = 0; i < 9; ++i) {
            REQUIRE(d->get(keys[i])->type() == types[i]);
        }

        bool found[9] = { };
        dict_t::iterator i1(d);
        for (int i = 0; i < 9; ++i) {
            REQUIRE(i1);
            auto key = i1.key_string();
            auto j = std::find(&keys[0], &keys[9], key) - &keys[0];
            REQUIRE(j < 9);
            REQUIRE_FALSE(found[j]);
            found[j] = true;
            REQUIRE(i1.value());
            REQUIRE(i1.value()->type() == types[j]);
            ++i1;
        }
        REQUIRE_FALSE(i1);

        md->remove("lo");
        REQUIRE_FALSE(d->get("lo"));

        md->remove_all();
        REQUIRE(d->count() == 0);
        dict_t::iterator i2(d);
        REQUIRE_FALSE(i2);
    }

    SECTION("copy") {
        auto ma = mutable_dict_t::new_dict();
        ma->set("a", 100);
        ma->set("b", slice_t("dog"));

        auto mb = mutable_dict_t::new_dict();
        mb->set("a", ma);
        REQUIRE(mb->get("a") == ma);

        auto mc = mutable_dict_t::new_dict();
        mc->set("a", mb);
        REQUIRE(mc->get("a") == mb);

        auto copy = mc->copy();
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE(copy->get("a") == mc->get("a"));

        copy = mc->copy(deep_copy);
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE_FALSE(copy->get("a") == mc->get("a"));
        REQUIRE_FALSE(copy->get("a")->as_dict()->get("a") == ma);
    }

    SECTION("copy immutable") {
        auto doc = doc_t::from_json("{\"a\":100,\"b\":\"dog\"}");
        const dict_t *a = doc->root()->as_dict();

        auto copy = mutable_dict_t::new_dict(a);
        REQUIRE(copy->source() == a);
        REQUIRE(copy->is_equal(a));

        auto mb = mutable_dict_t::new_dict();
        mb->set("a", a);
        REQUIRE(mb->get("a") == a);

        auto mc = mutable_dict_t::new_dict();
        mc->set("a", mb);
        REQUIRE(mc->get("a") == mb);

        copy = mc->copy();
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE(copy->get("a") == mc->get("a"));

        copy = mc->copy(deep_copy);
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE_FALSE(copy->get("a") == mc->get("a"));
        REQUIRE(copy->get("a")->as_dict()->get("a") == a);

        copy = mc->copy(copy_flags(deep_copy_immutables));
        REQUIRE_FALSE(copy == mc);
        REQUIRE(copy->is_equal(mc));
        REQUIRE_FALSE(copy->get("a") == mc->get("a"));
        REQUIRE_FALSE(copy->get("a")->as_dict()->get("a") == a);
    }

}


TEST_CASE("mutable long string") {
    constexpr uint32_t size = 50;
    const char *chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    auto ma = mutable_array_t::new_array(50);
    for (uint32_t i = 0; i < size; ++i) {
        ma->set(i, slice_t(chars, i));
    }
    for (uint32_t i = 0; i < size; ++i) {
        REQUIRE(ma->get(i)->as_string() == slice_t(chars, i));
    }
}

TEST_CASE("mutable_dict_t from file") {
    {
        auto data = json_coder::from_json(read_file("test/small-test.json"));
        write_to_file(data, "test/small-test.rj");
    }
    auto data = read_file("test/small-test.rj");
    auto doc = doc_t::from_slice(data, doc_t::trust_type::trusted);
    auto dog = doc->as_dict();
    auto mp = mutable_dict_t::new_dict(dog);
    REQUIRE(dog);
    mp->set("age", 7);
    auto achievements = mp->get_mutable_array("achievements");
    REQUIRE(achievements);
    auto achievement = achievements->get_mutable_dict(0);
    REQUIRE(achievement);
    REQUIRE(achievement->get("name")->as_string() == slice_t("He alwais get home"));
    achievement->set("name", slice_t("No achievements"));
    REQUIRE(achievement->get("name")->as_string() == slice_t("No achievements"));
}
