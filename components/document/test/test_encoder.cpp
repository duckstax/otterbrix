#include <catch2/catch.hpp>
#include <components/document/core/encoder.hpp>
#include <components/document/core/pointer.hpp>
#include <components/document/core/slice.hpp>
#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/core/path.hpp>
#include <components/document/json/json_coder.hpp>
#include <components/document/support/writer.hpp>
#include <components/document/support/slice_io.hpp>

using namespace document;
using namespace document::impl;
using document::slice_t;
using document::alloc_slice_t;

document::alloc_slice_t encoding_end(encoder_t &enc) {
    enc.end();
    auto res = enc.finish();
    REQUIRE(res);
    enc.reset();
    return res;
}

void require_hex(const alloc_slice_t &value, const std::string& compare_hex) {
    std::string hex;
    for (size_t i = 0; i < value.size; ++i) {
        char str[4];
        sprintf(str, "%02X", value[i]);
        hex.append(str);
        if (i % 2 && i != value.size - 1) {
            hex.append(" ");
        }
    }
    REQUIRE(hex == compare_hex);
}

void require_bool(const alloc_slice_t &value, bool b) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::boolean);
    REQUIRE(v->as_bool() == b);
}

void require_int(const alloc_slice_t &value, int64_t i) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::number);
    REQUIRE(v->is_int());
    REQUIRE_FALSE(v->is_unsigned());
    REQUIRE(v->as_int() == i);
}

void require_uint(const alloc_slice_t &value, uint64_t i) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::number);
    REQUIRE(v->is_int());
    REQUIRE(v->as_unsigned() == i);
}

void require_float(const alloc_slice_t &value, float f) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::number);
    REQUIRE_FALSE(v->is_double());
    REQUIRE(v->as_float() == Approx(f));
}

void require_double(const alloc_slice_t &value, double d) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::number);
    REQUIRE(v->as_double() == Approx(d));
}

void require_str(const alloc_slice_t &value, const slice_t &str) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::string);
    REQUIRE(v->as_string() == str);
}

const array_t* require_array(const alloc_slice_t &value, uint32_t count) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::array);
    auto array = v->as_array();
    REQUIRE(array);
    REQUIRE(array->count() == count);
    return array;
}

const dict_t* require_dict(const alloc_slice_t &value, uint32_t count) {
    auto v = value_t::from_data(value);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::dict);
    auto dict = v->as_dict();
    REQUIRE(dict);
    REQUIRE(dict->count() == count);
    return dict;
}


TEST_CASE("impl::encoder_t::empty") {
    encoder_t enc;
    REQUIRE(enc.empty());
    enc.begin_array();
    REQUIRE_FALSE(enc.empty());
    enc.end_array();
    enc.reset();
    REQUIRE(enc.empty());
    enc << 17;
    REQUIRE_FALSE(enc.empty());
    enc.reset();
    REQUIRE(enc.empty());
}

TEST_CASE("impl::encoder_t::pointer_t") {
    const uint8_t data[] = {0x80, 0x02};
    auto v = reinterpret_cast<const value_t *>(data);
    REQUIRE(v->as_pointer()->offset<false>() == 4u);
}

TEST_CASE("impl::encoder_t::special") {
    encoder_t enc;
    auto req = [&](bool b, const std::string &hex) {
        enc.write_bool(b);
        auto value = encoding_end(enc);
        require_hex(value, hex);
        require_bool(value, b);
    };

    enc.write_null();
    auto value = encoding_end(enc);
    require_hex(value, "3000");

    req(false, "3400");
    req(true, "3800");
}

TEST_CASE("impl::encoder_t::int") {
    encoder_t enc;
    auto req_int = [&](int64_t i, const std::string &hex = std::string()) {
        enc.write_int(i);
        auto value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        require_int(value, i);
    };
    auto req_uint = [&](uint64_t i, const std::string &hex = std::string()) {
        enc.write_uint(i);
        auto value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        require_uint(value, i);
    };

    req_int(0, "0000");
    req_int(128, "0080");
    req_int(1234, "04D2");
    req_int(-1234, "0B2E");
    req_int(2047, "07FF");
    req_int(-2048, "0800");
    req_int(2048, "1100 0800 8002");
    req_int(-2049, "11FF F700 8002");

    for (int64_t i = -66666; i <= 66666; ++i) req_int(i);
    for (uint64_t i = 0; i <= 66666; ++i) req_uint(i);

    req_int(12345678, "134E 61BC 0000 8003");
    req_int(-12345678, "13B2 9E43 FF00 8003");
    req_int(0x223344, "1244 3322 8002");
    req_int(0xBBCCDD, "13DD CCBB 0000 8003");
    req_int(0x11223344556677, "1677 6655 4433 2211 8004");
    req_int(0x1122334455667788, "1788 7766 5544 3322 1100 8005");
    req_int(-0x1122334455667788, "1778 8899 AABB CCDD EE00 8005");
    req_uint(0xCCBBAA9988776655, "1F55 6677 8899 AABB CC00 8005");
    req_uint(UINT64_MAX, "1FFF FFFF FFFF FFFF FF00 8005");

    for (int bits = 0; bits < 64; ++bits) {
        auto i = static_cast<int64_t>(1LL << bits);
        req_int(i);
        if (bits < 63) {
            req_int(-i);
            req_int(i - 1);
            req_int(1 - i);
        }
    }
    for (int bits = 0; bits < 64; ++bits) {
        uint64_t i = 1LLU << bits;
        req_uint(i);
        req_uint(i - 1);
    }
}

TEST_CASE("impl::encoder_t::float") {
    encoder_t enc;
    auto req_float = [&](float f, const std::string &hex = std::string()) {
        enc.write_float(f);
        auto value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        require_float(value, f);
    };
    auto req_double = [&](double d, const std::string &hex = std::string()) {
        enc.write_double(d);
        auto value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        require_double(value, d);
    };

    req_float(0.5, "2000 0000 003F 8003");
    req_double(0.5, "2000 0000 003F 8003");
    req_float(-0.5, "2000 0000 00BF 8003");
    req_double(-0.5, "2000 0000 00BF 8003");

    req_float(static_cast<float>(M_PI), "2000 DB0F 4940 8003");
    req_double(M_PI, "2800 182D 4454 FB21 0940 8005");
    req_double(static_cast<float>(M_PI), "2000 DB0F 4940 8003");
}

TEST_CASE("impl::encoder_t::string") {
    encoder_t enc;
    auto req_str = [&](const std::string &str, const std::string &hex = std::string()) {
        enc.write_string(str);
        auto value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        require_str(value, str);
    };

    req_str("", "4000");
    req_str("a", "4161");
    req_str("ab", "4261 6200 8002");
    req_str("abcdefghijklmn", "4E61 6263 6465 6667 6869 6A6B 6C6D 6E00 8008");
    req_str("abcdefghijklmno", "4F0F 6162 6364 6566 6768 696A 6B6C 6D6E 6F00 8009");
    req_str("abcdefghijklmnop", "4F10 6162 6364 6566 6768 696A 6B6C 6D6E 6F70 8009");
    req_str("müßchop", "496D C3BC C39F 6368 6F70 8005");
}

TEST_CASE("impl::encoder_t::array") {
    encoder_t enc;
    alloc_slice_t value;
    auto req_array = [&](uint32_t count, const std::string &hex = std::string()) {
        value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        return require_array(value, count);
    };

    enc.begin_array();
    enc.end_array();
    req_array(0, "6000");

    enc.begin_array(1);
    enc.write_null();
    enc.end_array();

    auto a = req_array(1, "6001 3000 8002");
    REQUIRE(a->type() == value_type::array);
    REQUIRE(a->count() == 1);
    auto v = a->get(0);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::null);

    enc.begin_array(2);
    enc.write_string("Hello");
    enc.write_string("world");
    enc.end_array();
    a = req_array(2, "4548 656C 6C6F 4577 6F72 6C64 6002 8007 8005 8003");
    v = a->get(0);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::string);
    REQUIRE(v->as_string() == "Hello");
    v = a->get(1);
    REQUIRE(v);
    REQUIRE(v->type() == value_type::string);
    REQUIRE(v->as_string() == "world");
    array_t::iterator iter(a);
    REQUIRE(iter);
    REQUIRE(iter->type() == value_type::string);
    REQUIRE(iter->as_string() == slice_t("Hello"));
    ++iter;
    REQUIRE(iter);
    REQUIRE(iter->type() == value_type::string);
    REQUIRE(iter->as_string() == slice_t("world"));
    ++iter;
    REQUIRE_FALSE(iter);
    REQUIRE(a->to_json() == alloc_slice_t("[\"Hello\",\"world\"]"));
}

TEST_CASE("impl::encoder_t::lenght array") {
    encoder_t enc;
    auto req_len = [&](uint32_t len) {
        enc.begin_array();
        for (uint32_t i = 0; i < len; ++i) enc.write_uint(i);
        enc.end_array();
        auto value = encoding_end(enc);
        auto a = require_array(value, len);
        for (uint32_t i = 0; i < len; ++i) {
            auto v = a->get(i);
            REQUIRE(v);
            REQUIRE(v->type() == value_type::number);
            REQUIRE(v->as_unsigned() == i);
        }
    };

    req_len(0x7FE);
    req_len(0x7FF);
    req_len(0x800);
    req_len(0x801);
    req_len(0xFFFF);
}

TEST_CASE("impl::encoder_t::dict") {
    encoder_t enc;
    alloc_slice_t value;
    auto req_dict = [&](uint32_t count, const std::string &hex = std::string()) {
        value = encoding_end(enc);
        if (!hex.empty()) require_hex(value, hex);
        return require_dict(value, count);
    };

    enc.begin_dict();
    enc.end_dict();
    req_dict(0, "7000");

    enc.begin_dict();
    enc.write_key("string");
    enc.write_int(100);
    enc.end_dict();
    auto d = req_dict(1, "4673 7472 696E 6700 7001 8005 0064 8003");
    auto v = d->get(slice_t("string"));
    REQUIRE(v);
    REQUIRE(v->as_int() == 100);
    REQUIRE_FALSE(d->get(slice_t("not_valid_key")));
    REQUIRE(d->to_json() == alloc_slice_t("{\"string\":100}"));
}

TEST_CASE("impl::encoder_t::deep") {
    encoder_t enc;
    for (int64_t depth = 0; depth < 100; ++depth) {
        enc.begin_array();
        enc.write_int(depth);
    }
    for (int64_t depth = 0; depth < 100; ++depth) {
        enc.write_string(std::string("Close: ") + std::to_string(depth));
        enc.end_array();
    }
    auto value = encoding_end(enc);
    require_array(value, 3);
}

TEST_CASE("impl::encoder_t::json string") {
    encoder_t enc;
    auto req_json = [&](std::string json, slice_t compare_value, const std::string& compare_error = std::string()) {
        json = std::string("[\"") + json + std::string("\"]");
        try {
            auto value = json_coder::from_json(enc, json);
            REQUIRE(value);
            auto a = require_array(value, 1);
            if (!compare_value.empty()) {
                auto str = a->get(0)->as_string();
                REQUIRE(str == compare_value);
            }
        }  catch (const document::exception_t &exception) {
            REQUIRE(exception.code == document::error_code::json_error);
            REQUIRE(std::string(exception.what()) == compare_error);
        }
        enc.reset();
    };

    req_json("", "");
    req_json("x", "x");
    req_json("\\\"", "\"");
    req_json("\"", nullptr, "JSON error: syntax error");
    req_json("\\", nullptr, "JSON error: incomplete JSON");
    req_json("hi \\\"there\\\"", "hi \"there\"");
    req_json("hi\\nthere", "hi\nthere");
    req_json("H\\u0061ppy", "Happy");
    req_json("H\\u0061", "Ha");

    req_json("Price 50\\u00A2", "Price 50¢");
    req_json("Price \\u20ac250", "Price €250");
    req_json("Price \\uffff?", "Price \uffff?");
    req_json("Price \\u20ac", "Price €");
    req_json("Price \\u20a", nullptr, "JSON error: expected hex digit");
    req_json("Price \\u20", nullptr, "JSON error: expected hex digit");
    req_json("Price \\u2", nullptr, "JSON error: expected hex digit");
    req_json("Price \\u", nullptr, "JSON error: expected hex digit");
    req_json("\\uzoop!", nullptr, "JSON error: expected hex digit");

    req_json("lmao\\uD83D\\uDE1C!", "lmao😜!");
    req_json("lmao\\uD83D", nullptr, "JSON error: syntax error");
    req_json("lmao\\uD83D\\n", nullptr, "JSON error: syntax error");
    req_json("lmao\\uD83D\\u", nullptr, "JSON error: expected hex digit");
    req_json("lmao\\uD83D\\u333", nullptr, "JSON error: expected hex digit");
    req_json("lmao\\uD83D\\u3333", nullptr, "JSON error: illegal trailing surrogate");
    req_json("lmao\\uDE1C\\uD83D!", nullptr, "JSON error: illegal leading surrogate");
}

TEST_CASE("impl::encoder_t::json") {
    encoder_t enc;
    slice_t json("{\"\":\"hello\\nt\\\\here\","
                     "\"\\\"ironic\\\"\":[null,false,true,-100,0,100,123.456,6.02e+23,5e-06],"
                     "\"foo\":123}");
    auto value = json_coder::from_json(enc, json);
    REQUIRE(value);
    enc.reset();
    auto d = require_dict(value, 3);
    REQUIRE(d->to_json() == json);
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

TEST_CASE("impl::encoder_t::json binary") {
    const alloc_slice_t message("not-really-binary");
    const alloc_slice_t code("bm90LXJlYWxseS1iaW5hcnk=");

    encoder_t enc;
    enc.begin_array();
    enc.write_data(slice_t(message));
    enc.end_array();
    auto value = encoding_end(enc);
    auto json = value_t::from_data(value)->to_json();
    REQUIRE(json == alloc_slice_t("[\"" + std::string(code) + "\"]"));

    document::writer_t w;
    w.write_decoded_base64(slice_t(code));
    REQUIRE(w.finish() == message);
}

TEST_CASE("impl::encoder_t::json from file") {
    SECTION("save into internal") {
        encoder_t enc;
        auto data = document::read_file("test/big-test.json");
        enc.unique_strings(true);
        auto value = json_coder::from_json(enc, data);
        REQUIRE(value.buf);
        document::write_to_file(value, "test/big-test.rj");
    }

    SECTION("internal from file") {
        auto doc = document::read_file("test/big-test.rj");
        auto value = value_t::from_trusted_data(doc);
        REQUIRE(value);
        REQUIRE(value->type() == value_type::array);
        REQUIRE(value->as_array());
    }

    SECTION("get in array and dict") {
        auto doc = document::read_file("test/big-test.rj");
        auto root = value_t::from_trusted_data(doc)->as_array();
        auto dog = root->get(6)->as_dict();
        REQUIRE(dog);
        auto name = dog->get("name");
        REQUIRE(name);
        REQUIRE(name->as_string() == slice_t("Toby"));
    }

    SECTION("path_t") {
        auto data = document::read_file("test/big-test.rj");
        const value_t* root = value_t::from_data(data);
        REQUIRE(root->as_array()->count() == 10);

        path_t p1{"$[3].name"};
        const value_t* name = p1.eval(root);
        REQUIRE(name);
        REQUIRE(name->type() == value_type::string);
        REQUIRE(name->as_string() == slice_t("Charlie"));

        path_t p2{"[-1].name"};
        name = p2.eval(root);
        REQUIRE(name);
        REQUIRE(name->type() == value_type::string);
        REQUIRE(name->as_string() == slice_t("Albert"));
    }
}

TEST_CASE("impl::encoder_t::multy item") {
    encoder_t enc;
    enc.suppress_trailer();
    size_t pos[10];
    unsigned n = 0;

    enc.begin_dict();
    enc.write_key("dog");
    enc.write_string("Rex");
    enc.end_dict();
    pos[n++] = enc.finish_item();

    enc.begin_dict();
    enc.write_key("age");
    enc.write_int(6);
    enc.end_dict();
    pos[n++] = enc.finish_item();

    enc.begin_array();
    enc.write_bool(false);
    enc.write_int(100);
    enc.end_array();
    pos[n++] = enc.finish_item();

    enc.write_string("sample string");
    pos[n++] = enc.finish_item();

    enc.write_int(20);
    pos[n++] = enc.finish_item();

    auto data = encoding_end(enc);
    pos[n] = data.size;
    for (unsigned i = 0; i < n; i++)
        REQUIRE(pos[i] < pos[i+1]);
    REQUIRE(data.size == pos[n]);

    auto dict = reinterpret_cast<const dict_t*>(&data[pos[0]]);
    REQUIRE(dict->type() == value_type::dict);
    REQUIRE(dict->count() == 1);
    REQUIRE(dict->get("dog"));
    REQUIRE(dict->get("dog")->type() == value_type::string);
    REQUIRE(dict->get("dog")->as_string() == "Rex");

    dict = reinterpret_cast<const dict_t*>(&data[pos[1]]);
    REQUIRE(dict->type() == value_type::dict);
    REQUIRE(dict->count() == 1);
    REQUIRE(dict->get("age"));
    REQUIRE(dict->get("age")->type() == value_type::number);
    REQUIRE(dict->get("age")->as_int() == 6);

    auto array = reinterpret_cast<const array_t*>(&data[pos[2]]);
    REQUIRE(array->type() == value_type::array);
    REQUIRE(array->count() == 2);
    REQUIRE(array->get(0)->type() == value_type::boolean);
    REQUIRE(array->get(0)->as_bool() == false);
    REQUIRE(array->get(1)->type() == value_type::number);
    REQUIRE(array->get(1)->as_int() == 100);

    auto str = reinterpret_cast<const value_t*>(&data[pos[3]]);
    REQUIRE(str->type() == value_type::string);
    REQUIRE(str->as_string() == "sample string");

    auto num = reinterpret_cast<const value_t*>(&data[pos[4]]);
    REQUIRE(num->type() == value_type::number);
    REQUIRE(num->as_int() == 20);
}

TEST_CASE("impl::encoder_t::rewriting value") {
    encoder_t enc;
    REQUIRE(enc.last_value_written() == encoder_t::pre_written_value::none);
    enc.begin_array();
    enc.begin_dict();
    enc.write_key("first");
    enc.write_int(100);
    REQUIRE(enc.last_value_written() == encoder_t::pre_written_value::none);
    enc.end_dict();
    REQUIRE_FALSE(enc.last_value_written() == encoder_t::pre_written_value::none);
    auto d1 = enc.last_value_written();
    enc.begin_dict();
    enc.write_key("second");
    enc.write_value_again(d1);
    enc.end_dict();
    enc.end_array();

    auto data = encoding_end(enc);
    require_hex(data, "4566 6972 7374 7001 8004 0064 4673 6563 6F6E 6400 7001 8005 8009 6002 800B 8005 8003");
    value_t::dump(data);
    auto root = value_t::from_data(data);
    REQUIRE(root);
    REQUIRE(root->to_json_string() == "[{\"first\":100},{\"second\":{\"first\":100}}]");
    auto dict1 = root->as_array()->get(0);
    auto dict2 = root->as_array()->get(1)->as_dict()->get("second");
    REQUIRE(dict1 == dict2);
}

TEST_CASE("impl::encoder_t::snip") {
    encoder_t enc;
    enc.begin_array();
    enc.begin_dict();
    enc.write_key("part");
    enc.write_int(1);
    enc.write_key("name");
    enc.write_string("part 1");
    enc.end_dict();
    alloc_slice_t part1 = enc.snip();

    REQUIRE(part1);
    auto value1 = value_t::from_data(part1);
    REQUIRE(value1);
    value_t::dump(part1);
    REQUIRE(value1->to_json_string() == R"({"name":"part 1","part":1})");

    enc.begin_dict();
    enc.write_key("part");
    enc.write_int(2);
    enc.write_key("name");
    enc.write_string("part 2");
    enc.end_dict();
    enc.end_array();
    auto data = encoding_end(enc);
    REQUIRE(data);

    document::retained_t<doc_t> doc(new doc_t(data, doc_t::trust_type::untrusted, nullptr, part1));
    REQUIRE(doc->root()->to_json_string() == R"([{"name":"part 1","part":1},{"name":"part 2","part":2}])");
    value_t::dump(data);
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

    SECTION("no error with internal encoder") {
        encoder_t enc;
        REQUIRE(document::impl::json_coder::from_json("{}"));
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
