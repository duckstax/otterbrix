#include <catch2/catch.hpp>
#include <components/document/core/pointer.hpp>
#include <components/document/core/slice.hpp>
#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/json/json_coder.hpp>
#include <components/document/support/writer.hpp>
#include <components/document/support/slice_io.hpp>
#include <components/document/support/num_conversion.hpp>

using namespace document;
using namespace document::impl;
using document::slice_t;
using document::alloc_slice_t;

TEST_CASE("impl::encoder_t::pointer_t") {
    const uint8_t data[] = {0x80, 0x02};
    auto v = reinterpret_cast<const value_t *>(data);
    REQUIRE(v->as_pointer()->offset<false>() == 4u);
}


TEST_CASE("impl::encoder_t::json number") {
    slice_t json = "[9223372036854775807, -9223372036854775808, 18446744073709551615, "
                       "18446744073709551616, 602214076000000000000000, -9999999999999999999]";
    auto data = json_coder::from_json(json);
    const array_t *root = value_t::from_trusted_data(data)->as_array();
    REQUIRE(root->get(0)->is_int());
    REQUIRE(root->get(0)->as_int() == INT64_MAX);
    REQUIRE(root->get(1)->is_int());
    REQUIRE(root->get(1)->as_int() == INT64_MIN);
    REQUIRE(root->get(2)->is_int());
    REQUIRE(root->get(2)->as_unsigned() == UINT64_MAX);
    REQUIRE_FALSE(root->get(3)->is_int());
    REQUIRE(root->get(3)->as_double() == Approx(18446744073709551616.0));
    REQUIRE_FALSE(root->get(4)->is_int());
    REQUIRE(root->get(4)->as_double() == Approx(6.02214076e23));
    REQUIRE_FALSE(root->get(5)->is_int());
    REQUIRE(root->get(5)->as_double() == Approx(-9999999999999999999.0));
}

TEST_CASE("impl::encoder_t::double encoding") {
    double d = M_PI;
    float f = 2.71828f;
    char buf_d[32];
    char buf_f[32];
    sprintf(buf_d, "%.16g", d);
    sprintf(buf_f, "%.7g", f);
    REQUIRE(std::string(buf_d) == "3.141592653589793");
    REQUIRE(std::string(buf_f) == "2.71828");

    document::write_float(d, buf_d, 32);
    document::write_float(f, buf_f, 32);
    REQUIRE(std::string(buf_d) == "3.141592653589793");
    REQUIRE(std::string(buf_f) == "2.71828");

    REQUIRE(document::parse_double(buf_d) == Approx(M_PI));
    REQUIRE(static_cast<float>(document::parse_double(buf_f)) == Approx(2.71828f));
}

TEST_CASE("impl::encoder_t::uint encoding") {
    constexpr const char* test_values[] = {"0", "1", "9", "  99 ", "+12345", "  +12345", "18446744073709551615"};
    for (const char *str : test_values) {
        uint64_t value;
        REQUIRE(document::parse_integer(str, value));
        REQUIRE(value == strtoull(str, nullptr, 10));
    }

    constexpr const char* fail_test_values[] = {"", " ", "+", "x", "1234x", "1234 x", "123.456", "-17", " + 1234", "18446744073709551616"};
    for (const char *str : fail_test_values) {
        uint64_t value;
        REQUIRE_FALSE(document::parse_integer(str, value));
    }
}

TEST_CASE("impl::encoder_t::int encoding") {
    constexpr const char* test_values[] = {"0", "1", "9", "  99 ", "+17", "+0", "-0", "-1", "+12", " -12345",
                                           "9223372036854775807", "-9223372036854775808"};
    for (const char *str : test_values) {
        int64_t value;
        REQUIRE(document::parse_integer(str, value));
        REQUIRE(value == strtoll(str, nullptr, 10));
    }

    constexpr const char* fail_test_values[] = {"", " ", "x", " x", "1234x", "1234 x", "123.456", "18446744073709551616",
                                                "-", " - ", "-+", "- 1", "9223372036854775808", "-9223372036854775809"};
    for (const char *str : fail_test_values) {
        int64_t value;
        REQUIRE_FALSE(document::parse_integer(str, value));
    }
}


TEST_CASE("JSON") {

    SECTION("incomplete") {
        try {
            document::impl::json_coder::from_json("{");
        } catch (const exception_t &exception) {
            REQUIRE(exception.code == document::error_code::json_error);
            REQUIRE(std::string(exception.what()) == "JSON error: incomplete JSON");
        }
    }

    SECTION("decode to json") {
        json_encoder_t enc;
        enc.begin_dict();
        enc.write_key(slice_t("int"));
        enc.write_int(100);
        enc.write_key(slice_t("str"));
        enc.write_string(slice_t("abc"));
        enc.end_dict();
        auto data = enc.finish();
        REQUIRE(data.as_string() == R"r({"int":100,"str":"abc"})r");
    }

}
