#include <catch2/catch.hpp>

#include <fstream>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <vector>
#include <string>

#include "components/document/serializer.hpp"
#include "components/document/document.hpp"
#include "components/document/document_view.hpp"


#include <components/tests/generaty.hpp>

using namespace components::document;

/*
namespace {
    class SaxCountdown {
    public:
        explicit SaxCountdown(const int count)
            : events_left(count) {}

        bool null() {
            return events_left-- > 0;
        }

        bool boolean(bool) {
            return events_left-- > 0;
        }

        bool number_integer(document_t::number_integer_t ) {
            return events_left-- > 0;
        }

        bool number_unsigned(document_t::number_unsigned_t ) {
            return events_left-- > 0;
        }

        bool number_float(document_t::number_float_t , const std::string& ) {
            return events_left-- > 0;
        }

        bool string(std::string& ) {
            return events_left-- > 0;
        }

        bool binary(std::vector<std::uint8_t>& ) {
            return events_left-- > 0;
        }

        bool start_object(std::size_t ) {
            return events_left-- > 0;
        }

        bool key(std::string& ) {
            return events_left-- > 0;
        }

        bool end_object() {
            return events_left-- > 0;
        }

        bool start_array(std::size_t ) {
            return events_left-- > 0;
        }

        bool end_array() {
            return events_left-- > 0;
        }

        bool parse_error(std::size_t , const std::string& , const document_t::exception& ) // NOLINT(readability-convert-member-functions-to-static)
        {
            return false;
        }

    private:
        int events_left = 0;
    };
} // namespace
*/
TEST_CASE("MessagePack") {
    SECTION("individual values") {
        /* ABANDONED
        SECTION("undefined") {
            // undefined values are not serialized
            
            //const auto result = to_msgpack(make_document(document::impl::value_t::undefined_value));
            const auto result = to_msgpack(make_document());
            CHECK(result.empty());
        }*/


        SECTION("null") {
            std::vector<uint8_t> const expected = {0x91, 0xc0};
            const auto result = to_msgpack(gen_special_doc('n'));
            CHECK(result == expected);

            // roundtrip
            //CHECK(from_msgpack(result) == docView);
            //CHECK(from_msgpack(result, true, false) == docView);
        }

        SECTION("boolean") {
            SECTION("true") {
                std::vector<uint8_t> const expected = {0x91, 0xc3};
                const auto result = to_msgpack(gen_special_doc('t'));
                CHECK(result == expected);

                // roundtrip
                //CHECK(is_equals_documents(from_msgpack(result), doc));
                //CHECK(from_msgpack(result, true, false) == docView);
            }

            SECTION("false") {
                std::vector<uint8_t> const expected = {0x91, 0xc2};
                const auto result = to_msgpack(gen_special_doc('f'));
                CHECK(result == expected);

                // roundtrip
                //CHECK(is_equals_documents(from_msgpack(result), doc));
                //CHECK(from_msgpack(result, true, false) == docView);
            }
        }

        SECTION("number") {
            SECTION("integer") {
                SECTION("-32..127 (fixnum)") {



                    for (int16_t i = -32; i <= 127; ++i) {
                        CAPTURE(i);
                        
                        document_ptr doc = gen_number_doc(i);

                        std::vector<unsigned char> expected = {0x91}; // array + fixnum
                        uint8_t buffer[sizeof(int8_t)];

                        intToBytes<int8_t>(i, buffer);
                        expected.insert((expected.begin() + 1), buffer, buffer + sizeof(buffer));
                        
                        // compare result + size
                        const auto result = to_msgpack(doc);
                        CHECK(result == expected);
                        CHECK(result.size() == 2);

                        CHECK(is_equals_documents(from_msgpack(result), doc));
                        // roundtrip
                        //CHECK(from_msgpack(result) == j);
                        //CHECK(from_msgpack(result, true, false) == j);
                    }
                }

                SECTION("-128..127 (int 8)") {

                }

                SECTION("-32768..-129 -- 128..32767 (int 16)") {



                    for (int32_t i = -32768; i <= -129; i++){
                        CAPTURE(i);


                        document_ptr doc = gen_number_doc(i);
                        
                        std::vector<unsigned char> expected = {0x91, 0xD1}; // array + int16
                        uint8_t buffer[sizeof(int16_t)];

                        intToBytes<int16_t>(i, buffer);
                        expected.insert(expected.end(), buffer, buffer + sizeof(buffer));

                        // compare result + size
                        const auto result = to_msgpack(doc);
                        CHECK(result == expected);
                        CHECK(result.size() == 4);

                    }
                    for (int32_t i = 128; i <= 32767; ++i) {
                        CAPTURE(i);
                        
                        
                        document_ptr doc = gen_number_doc(i);
                        
                        std::vector<unsigned char> expected = {0x91, 0xD1}; // array + int16
                        uint8_t buffer[sizeof(int16_t)];

                        intToBytes<int16_t>(i, buffer);
                        expected.insert(expected.end(), buffer, buffer + sizeof(buffer));

                        // compare result + size
                        const auto result = to_msgpack(doc);
                        CHECK(result == expected);
                        CHECK(result.size() == 4);

                        // roundtrip
                        //CHECK(from_msgpack(result) == j);
                        //CHECK(from_msgpack(result, true, false) == j);
                    }
                }

                SECTION("-2147483648..2147483647  (int 32)") {



                    for (int32_t i :
                         std::initializer_list<int32_t> {
                            -65536, -77777, -1048576, -2147483648, 65536, 77777, 1048576, 2147483647
                         }) 
                    {
                        
                        CAPTURE(i);
                        
                        document_ptr doc = gen_number_doc(i);

                        std::vector<unsigned char> expected = {0x91, 0xD2};
                        uint8_t buffer[sizeof(int32_t)];

                        intToBytes<int32_t>(i, buffer);
                        expected.insert(expected.end(), buffer, buffer + sizeof(buffer));
                        
                        // compare result + size
                        const auto result = to_msgpack(doc);
                        CHECK(result == expected);
                        CHECK(result.size() == 6);

                        // roundtrip
                        //CHECK(from_msgpack(result) == j);
                        //CHECK(from_msgpack(result, true, false) == j);
                    }
                }

                SECTION("-9223372036854775808..9223372036854775807 (int 64)") {



                    for (int64_t i :
                        std::initializer_list<int64_t> {
                            -4294967296, -9223372036854775807, -4294967296, 9223372036854775807
                        }) 
                    {
                        CAPTURE(i);
                        
                        document_ptr doc = gen_number_doc(i);

                        std::vector<unsigned char> expected = {0x91, 0xD3};
                        uint8_t buffer[sizeof(int64_t)];

                        intToBytes<int64_t>(i, buffer);
                        expected.insert(expected.end(), buffer, buffer + sizeof(buffer));

                        // compare result + size
                        const auto result = to_msgpack(doc);
                        CHECK(result == expected);
                        CHECK(result.size() == 10);

                        // roundtrip
                        //CHECK(from_msgpack(result) == j);
                        //CHECK(from_msgpack(result, true, false) == j);
                    }
                }
            }
            SECTION("unsigned") {
                SECTION("0..255 (uint 8)") {
                    document_ptr doc = make_document();
                    doc->set("value", u_int64_t(10));

                    document_view_t docView(doc);
                    CHECK(docView.is_ulong("value"));
                    /*
                    std::vector<unsigned char> expected = {0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xcc}; // "value = "
                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);

                    uint8_t buffer[sizeof(uint8_t)];

                    for (uint8_t i = 0; i <= 254; i++){
                        doc->set("value", i);

                        intToBytes<uint8_t>(i, buffer);
                        expected.insert(expected.end(), buffer, buffer + sizeof(buffer));

                        const auto result = to_msgpack(doc);
                        CHECK(result == expected);
                        CHECK(result.size() == 9);

                        expected.erase(expected.end() - 1, expected.end());
                    } 
                    */
                }

            }

            SECTION("float") {
                SECTION("3.1415925") {

                    document_ptr doc = make_document();
                    doc->set("value", double(3.1415925));

                    document_view_t docView(doc);
                    CHECK(docView.is_double("value"));
                    std::vector<uint8_t> const expected =
                        {
                            0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xcb, 0x40, 0x09, 0x21, 0xfb, 0x3f, 0xa6, 0xde, 0xfc };
                    
                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == 16);

                    // roundtrip
                    //CHECK(from_msgpack(result) == j);
                    //CHECK(from_msgpack(result) == v);
                    //CHECK(from_msgpack(result, true, false) == j);
                }

                SECTION("1.0") {
                    document_ptr doc = make_document();
                    doc->set("value", double(1));

                    document_view_t docView(doc);
                    CHECK(docView.is_double("value"));

                    std::vector<uint8_t> const expected =
                        {
                            0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xcb, 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == 16);

                    // roundtrip
                    //auto roundtrip = from_msgpack(result);
                    //CHECK(from_msgpack(result) == v);
                    //CHECK(from_msgpack(result, true, false) == j);
                }

                SECTION("128.128") {
                    document_ptr doc = make_document();
                    doc->set("value", double(128.1280059814453125));

                    std::vector<uint8_t> const expected =
                        {
                            0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xcb, 0x40, 0x60, 0x04, 0x18, 0xa0, 0x00, 0x00, 0x00};

                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == 16);

                    // roundtrip
                    //CHECK(from_msgpack(result) == j);
                    //CHECK(from_msgpack(result) == v);
                    //CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("string") {

            SECTION("N = 0..31") {


                const std::vector<uint8_t> first_bytes =
                    {
                        0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8,
                        0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0, 0xb1,
                        0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba,
                        0xbb, 0xbc, 0xbd, 0xbe, 0xbf};
                
                document_ptr doc = make_document();
                
                for (size_t N = 1; N < first_bytes.size(); ++N) {
                    CAPTURE(N);

                    const auto s = std::string(N, 'x');

                    doc->set("value", s);


                    // create expected byte vector
                    std::vector<uint8_t> expected = {0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65};
                    expected.push_back(first_bytes[N]);

                    for (size_t i = 0; i < N; ++i) {
                        expected.push_back('x');
                    }

                    // compare result + size
                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 8);

                    // check that no null byte is appended
                    if (N > 0) {
                        CHECK(result.back() != '\x00');
                    }

                    // roundtrip
                    CHECK(is_equals_documents(from_msgpack(result), doc));
                    //CHECK(from_msgpack(result) == j);
                    //CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 32..255") {

                document_ptr doc = make_document();

                for (size_t N = 32; N <= 255; ++N) {
                    CAPTURE(N);

                    const auto s = std::string(N, 'x');

                    doc->set("value", s);


                    // create expected byte vector
                    std::vector<uint8_t> expected = {0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xd9};
                    expected.push_back(static_cast<uint8_t>(N));

                    for (size_t i = 0; i < N; ++i) {
                        expected.push_back('x');
                    }

                    // compare result + size
                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 9);

                    // check that no null byte is appended
                    if (N > 0) {
                        CHECK(result.back() != '\x00');
                    }


                    // roundtrip
                    CHECK(is_equals_documents(from_msgpack(result), doc));
                    //CHECK(from_msgpack(result) == j);
                    //CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 256..65535") {
                document_ptr doc = make_document();

                for (uint16_t N :
                     {
                         256u, 999u, 1025u, 3333u, 2048u, 65535u}) {
                    CAPTURE(N);

                    const auto s = std::string(N, 'x');

                    doc->set("value", s);


                    // create expected byte vector
                    std::vector<uint8_t> expected = {0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xda};
                    uint8_t buffer[sizeof(uint16_t)];
                    intToBytes<uint16_t>(N, buffer);
                    expected.insert(expected.end(), buffer, buffer + sizeof(buffer));

                    for (size_t i = 0; i < N; ++i) {
                        expected.push_back('x');
                    }

                    // compare result + size
                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 10);

                    // check that no null byte is appended
                    if (N > 0) {
                        CHECK(result.back() != '\x00');
                    }


                    // roundtrip
                    CHECK(is_equals_documents(from_msgpack(result), doc));
                    //CHECK(from_msgpack(result) == j);
                    //CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 65536..4294967295") {
                document_ptr doc = make_document();

                for (uint32_t N :
                     {
                         65536u, 77777u, 1048576u}) {
                    CAPTURE(N);

                    const auto s = std::string(N, 'x');

                    doc->set("value", s);


                    // create expected byte vector
                    std::vector<uint8_t> expected = {0x81, 0xa5, 0x76, 0x61, 0x6c, 0x75, 0x65, 0xdb};
                    uint8_t buffer[sizeof(uint32_t)];
                    intToBytes<uint32_t>(N, buffer);
                    expected.insert(expected.end(), buffer, buffer + sizeof(buffer));

                    for (size_t i = 0; i < N; ++i) {
                        expected.push_back('x');
                    }

                    // compare result + size
                    const auto result = to_msgpack(doc);
                    CHECK(result == expected);
                    CHECK(result.size() == expected.size());

                    // check that no null byte is appended
                    if (N > 0) {
                        CHECK(result.back() != '\x00');
                    }


                    // roundtrip
                    CHECK(is_equals_documents(from_msgpack(result), doc));
                    //CHECK(from_msgpack(result) == j);
                    //CHECK(from_msgpack(result, true, false) == j);
                }
            }
        }

        }
    }
}
/*


        SECTION("array") {
            SECTION("empty") {
                document_t const j = document_t::array();
                std::vector<uint8_t> const expected = {0x90};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("[null]") {
                document_t const j = {nullptr};
                std::vector<uint8_t> const expected = {0x91, 0xc0};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("[1,2,3,4,5]") {
                auto const j = from_json("[1,2,3,4,5]");
                std::vector<uint8_t> const expected = {0x95, 0x01, 0x02, 0x03, 0x04, 0x05};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("[[[[]]]]") {
                auto const j = from_json("[[[[]]]]");
                std::vector<uint8_t> const expected = {0x91, 0x91, 0x91, 0x90};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("array 16") {
                document_t j(16, nullptr);
                std::vector<uint8_t> expected(j.size() + 3, 0xc0); // all null
                expected[0] = 0xdc;                                // array 16
                expected[1] = 0x00;                                // size (0x0010), byte 0
                expected[2] = 0x10;                                // size (0x0010), byte 1
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("array 32") {
                document_t j(65536, nullptr);
                std::vector<uint8_t> expected(j.size() + 5, 0xc0); // all null
                expected[0] = 0xdd;                                // array 32
                expected[1] = 0x00;                                // size (0x00100000), byte 0
                expected[2] = 0x01;                                // size (0x00100000), byte 1
                expected[3] = 0x00;                                // size (0x00100000), byte 2
                expected[4] = 0x00;                                // size (0x00100000), byte 3
                const auto result = to_msgpack(j);
                //CHECK(result == expected);

                CHECK(result.size() == expected.size());
                for (size_t i = 0; i < expected.size(); ++i) {
                    CAPTURE(i)
                    CHECK(result[i] == expected[i]);
                }

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }
        }

        SECTION("object") {
            SECTION("empty") {
                document_t const j = document_t::object();
                std::vector<uint8_t> const expected = {0x80};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("{\"\":null}") {
                document_t const j = {{"", nullptr}};
                std::vector<uint8_t> const expected = {0x81, 0xa0, 0xc0};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("{\"a\": {\"b\": {\"c\": {}}}}") {
                document_t const j = from_json(R"({"a": {"b": {"c": {}}}})");
                std::vector<uint8_t> const expected =
                    {
                        0x81, 0xa1, 0x61, 0x81, 0xa1, 0x62, 0x81, 0xa1, 0x63, 0x80};
                const auto result = to_msgpack(j);
                CHECK(result == expected);

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("map 16") {
                document_t const j = R"({"00": null, "01": null, "02": null, "03": null,
                             "04": null, "05": null, "06": null, "07": null,
                             "08": null, "09": null, "10": null, "11": null,
                             "12": null, "13": null, "14": null, "15": null})"_json;

                const auto result = to_msgpack(j);

                // Checking against an expected vector byte by byte is
                // difficult, because no assumption on the order of key/value
                // pairs are made. We therefore only check the prefix (type and
                // size and the overall size. The rest is then handled in the
                // roundtrip check.
                CHECK(result.size() == 67); // 1 type, 2 size, 16*4 content
                CHECK(result[0] == 0xde);   // map 16
                CHECK(result[1] == 0x00);   // byte 0 of size (0x0010)
                CHECK(result[2] == 0x10);   // byte 1 of size (0x0010)

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }

            SECTION("map 32") {
                document_t j;
                for (auto i = 0; i < 65536; ++i) {
                    // format i to a fixed width of 5
                    // each entry will need 7 bytes: 6 for fixstr, 1 for null
                    std::stringstream ss;
                    ss << std::setw(5) << std::setfill('0') << i;
                    j.emplace(ss.str(), nullptr);
                }

                const auto result = to_msgpack(j);

                // Checking against an expected vector byte by byte is
                // difficult, because no assumption on the order of key/value
                // pairs are made. We therefore only check the prefix (type and
                // size and the overall size. The rest is then handled in the
                // roundtrip check.
                CHECK(result.size() == 458757); // 1 type, 4 size, 65536*7 content
                CHECK(result[0] == 0xdf);       // map 32
                CHECK(result[1] == 0x00);       // byte 0 of size (0x00010000)
                CHECK(result[2] == 0x01);       // byte 1 of size (0x00010000)
                CHECK(result[3] == 0x00);       // byte 2 of size (0x00010000)
                CHECK(result[4] == 0x00);       // byte 3 of size (0x00010000)

                // roundtrip
                CHECK(from_msgpack(result) == j);
                CHECK(from_msgpack(result, true, false) == j);
            }
        }

        SECTION("extension") {
            SECTION("N = 0..255") {
                for (size_t N = 0; N <= 0xFF; ++N) {
                    CAPTURE(N)

                    const auto s = std::vector<uint8_t>(N, 'x');
                    document_t j = document_t::binary(s);
                    std::uint8_t const subtype = 42;
                    j.get_binary().set_subtype(subtype);

                    // create expected byte vector
                    std::vector<uint8_t> expected;
                    switch (N) {
                        case 1:
                            expected.push_back(static_cast<std::uint8_t>(0xD4));
                            break;
                        case 2:
                            expected.push_back(static_cast<std::uint8_t>(0xD5));
                            break;
                        case 4:
                            expected.push_back(static_cast<std::uint8_t>(0xD6));
                            break;
                        case 8:
                            expected.push_back(static_cast<std::uint8_t>(0xD7));
                            break;
                        case 16:
                            expected.push_back(static_cast<std::uint8_t>(0xD8));
                            break;
                        default:
                            expected.push_back(static_cast<std::uint8_t>(0xC7));
                            expected.push_back(static_cast<std::uint8_t>(N));
                            break;
                    }
                    expected.push_back(subtype);

                    for (size_t i = 0; i < N; ++i) {
                        expected.push_back(0x78);
                    }

                    // compare result + size
                    const auto result = to_msgpack(j);
                    CHECK(result == expected);
                    switch (N) {
                        case 1:
                        case 2:
                        case 4:
                        case 8:
                        case 16:
                            CHECK(result.size() == N + 2);
                            break;
                        default:
                            CHECK(result.size() == N + 3);
                            break;
                    }

                    // check that no null byte is appended
                    if (N > 0) {
                        CHECK(result.back() != '\x00');
                    }

                    // roundtrip
                    CHECK(from_msgpack(result) == j);
                    CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 256..65535") {
                for (std::size_t N :
                     {
                         256u, 999u, 1025u, 3333u, 2048u, 65535u}) {
                    CAPTURE(N)


                    const auto s = std::vector<uint8_t>(N, 'x');
                    document_t j = document_t::binary(s);
                    std::uint8_t const subtype = 42;
                    j.get_binary().set_subtype(subtype);

                    // create expected byte vector (hack: create string first)
                    std::vector<uint8_t> expected(N, 'x');
                    // reverse order of commands, because we insert at begin()
                    expected.insert(expected.begin(), subtype);
                    expected.insert(expected.begin(), static_cast<uint8_t>(N & 0xff));
                    expected.insert(expected.begin(), static_cast<uint8_t>((N >> 8) & 0xff));
                    expected.insert(expected.begin(), 0xC8);

                    // compare result + size
                    const auto result = to_msgpack(j);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 4);
                    // check that no null byte is appended
                    CHECK(result.back() != '\x00');

                    // roundtrip
                    CHECK(from_msgpack(result) == j);
                    CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 65536..4294967295") {
                for (std::size_t N :
                     {
                         65536u, 77777u, 1048576u}) {
                    CAPTURE(N)


                    const auto s = std::vector<uint8_t>(N, 'x');
                    document_t j = document_t::binary(s);
                    std::uint8_t const subtype = 42;
                    j.get_binary().set_subtype(subtype);

                    // create expected byte vector (hack: create string first)
                    std::vector<uint8_t> expected(N, 'x');
                    // reverse order of commands, because we insert at begin()
                    expected.insert(expected.begin(), subtype);
                    expected.insert(expected.begin(), static_cast<uint8_t>(N & 0xff));
                    expected.insert(expected.begin(), static_cast<uint8_t>((N >> 8) & 0xff));
                    expected.insert(expected.begin(), static_cast<uint8_t>((N >> 16) & 0xff));
                    expected.insert(expected.begin(), static_cast<uint8_t>((N >> 24) & 0xff));
                    expected.insert(expected.begin(), 0xC9);

                    // compare result + size
                    const auto result = to_msgpack(j);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 6);
                    // check that no null byte is appended
                    CHECK(result.back() != '\x00');

                    // roundtrip
                    CHECK(from_msgpack(result) == j);
                    CHECK(from_msgpack(result, true, false) == j);
                }
            }
        }

        SECTION("binary") {
            SECTION("N = 0..255") {
                for (std::size_t N = 0; N <= 0xFF; ++N) {
                    CAPTURE(N)

                    const auto s = std::vector<uint8_t>(N, 'x');
                    document_t const j = document_t::binary(s);

                    // create expected byte vector
                    std::vector<std::uint8_t> expected;
                    expected.push_back(static_cast<std::uint8_t>(0xC4));
                    expected.push_back(static_cast<std::uint8_t>(N));
                    for (size_t i = 0; i < N; ++i) {
                        expected.push_back(0x78);
                    }

                    // compare result + size
                    const auto result = to_msgpack(j);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 2);
                    // check that no null byte is appended
                    if (N > 0) {
                        CHECK(result.back() != '\x00');
                    }

                    // roundtrip
                    CHECK(from_msgpack(result) == j);
                    CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 256..65535") {
                for (std::size_t N :
                     {
                         256u, 999u, 1025u, 3333u, 2048u, 65535u}) {
                    CAPTURE(N)


                    const auto s = std::vector<std::uint8_t>(N, 'x');
                    document_t const j = document_t::binary(s);

                    // create expected byte vector (hack: create string first)
                    std::vector<std::uint8_t> expected(N, 'x');
                    // reverse order of commands, because we insert at begin()
                    expected.insert(expected.begin(), static_cast<std::uint8_t>(N & 0xff));
                    expected.insert(expected.begin(), static_cast<std::uint8_t>((N >> 8) & 0xff));
                    expected.insert(expected.begin(), 0xC5);

                    // compare result + size
                    const auto result = to_msgpack(j);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 3);
                    // check that no null byte is appended
                    CHECK(result.back() != '\x00');

                    // roundtrip
                    CHECK(from_msgpack(result) == j);
                    CHECK(from_msgpack(result, true, false) == j);
                }
            }

            SECTION("N = 65536..4294967295") {
                for (std::size_t N :
                     {
                         65536u, 77777u, 1048576u}) {
                    CAPTURE(N)


                    const auto s = std::vector<std::uint8_t>(N, 'x');
                    document_t const j = document_t::binary(s);

                    // create expected byte vector (hack: create string first)
                    std::vector<uint8_t> expected(N, 'x');
                    // reverse order of commands, because we insert at begin()
                    expected.insert(expected.begin(), static_cast<std::uint8_t>(N & 0xff));
                    expected.insert(expected.begin(), static_cast<std::uint8_t>((N >> 8) & 0xff));
                    expected.insert(expected.begin(), static_cast<std::uint8_t>((N >> 16) & 0xff));
                    expected.insert(expected.begin(), static_cast<std::uint8_t>((N >> 24) & 0xff));
                    expected.insert(expected.begin(), 0xC6);

                    // compare result + size
                    const auto result = to_msgpack(j);
                    CHECK(result == expected);
                    CHECK(result.size() == N + 5);
                    // check that no null byte is appended
                    CHECK(result.back() != '\x00');

                    // roundtrip
                    CHECK(from_msgpack(result) == j);
                    CHECK(from_msgpack(result, true, false) == j);
                }
            }
        }
    }

    SECTION("from float32") {
        auto given = std::vector<uint8_t>({0xca, 0x41, 0xc8, 0x00, 0x01});
        document_t const j = from_msgpack(given);
        CHECK(j.get<double>() == Approx(25.0000019073486));
    }

    SECTION("errors") {
        SECTION("empty byte vector") {
            document_t _;
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>()), "[json.exception.parse_error.110] parse error at byte 1: syntax error while parsing MessagePack value: unexpected end of input", json::parse_error&);
            CHECK(from_msgpack(std::vector<uint8_t>(), true, false).is_discarded());
        }

        SECTION("too short byte vector") {
            document_t _;

            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0x87})),
                                 "[json.exception.parse_error.110] parse error at byte 2: syntax error while parsing MessagePack string: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcc})),
                                 "[json.exception.parse_error.110] parse error at byte 2: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcd})),
                                 "[json.exception.parse_error.110] parse error at byte 2: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcd, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 3: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xce})),
                                 "[json.exception.parse_error.110] parse error at byte 2: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xce, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 3: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xce, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 4: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xce, 0x00, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 5: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf})),
                                 "[json.exception.parse_error.110] parse error at byte 2: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 3: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 4: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 5: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 6: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 7: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 8: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})),
                                 "[json.exception.parse_error.110] parse error at byte 9: syntax error while parsing MessagePack number: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xa5, 0x68, 0x65})),
                                 "[json.exception.parse_error.110] parse error at byte 4: syntax error while parsing MessagePack string: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0x92, 0x01})),
                                 "[json.exception.parse_error.110] parse error at byte 3: syntax error while parsing MessagePack value: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0x81, 0xa1, 0x61})),
                                 "[json.exception.parse_error.110] parse error at byte 4: syntax error while parsing MessagePack value: unexpected end of input", json::parse_error&);
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xc4, 0x02})),
                                 "[json.exception.parse_error.110] parse error at byte 3: syntax error while parsing MessagePack binary: unexpected end of input", json::parse_error&);

            CHECK(from_msgpack(std::vector<uint8_t>({0x87}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcc}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcd}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcd, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xce}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xce, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xce, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xce, 0x00, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xa5, 0x68, 0x65}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0x92, 0x01}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0x81, 0xA1, 0x61}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xc4, 0x02}), true, false).is_discarded());
            CHECK(from_msgpack(std::vector<uint8_t>({0xc4}), true, false).is_discarded());
        }

        SECTION("unsupported bytes") {
            SECTION("concrete examples") {
                document_t _;
                CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0xc1})), "[json.exception.parse_error.112] parse error at byte 1: syntax error while parsing MessagePack value: invalid byte: 0xC1", json::parse_error&);
            }

            SECTION("all unsupported bytes") {
                for (auto byte :
                     {
                         // never used
                         0xc1}) {
                    document_t _;
                    CHECK_THROWS_AS(_ = from_msgpack(std::vector<uint8_t>({static_cast<uint8_t>(byte)})), json::parse_error&);
                    CHECK(from_msgpack(std::vector<uint8_t>({static_cast<uint8_t>(byte)}), true, false).is_discarded());
                }
            }
        }

        SECTION("invalid string in map") {
            document_t _;
            CHECK_THROWS_WITH_AS(_ = from_msgpack(std::vector<uint8_t>({0x81, 0xff, 0x01})), "[json.exception.parse_error.113] parse error at byte 2: syntax error while parsing MessagePack string: expected length specification (0xA0-0xBF, 0xD9-0xDB); last byte: 0xFF", json::parse_error&);
            CHECK(from_msgpack(std::vector<uint8_t>({0x81, 0xff, 0x01}), true, false).is_discarded());
        }

        SECTION("strict mode") {
            std::vector<uint8_t> const vec = {0xc0, 0xc0};
            SECTION("non-strict mode") {
                const auto result = from_msgpack(vec, false);
                CHECK(result == document_t());
            }

            SECTION("strict mode") {
                document_t _;
                CHECK_THROWS_WITH_AS(_ = from_msgpack(vec), "[document_t.exception.parse_error.110] parse error at byte 2: syntax error while parsing MessagePack value: expected end of input; last byte: 0xC0", json::parse_error&);
                CHECK(from_msgpack(vec, true, false).is_discarded());
            }
        }
    }

    SECTION("SAX aborts") {
        SECTION("start_array(len)") {
            std::vector<uint8_t> const v = {0x93, 0x01, 0x02, 0x03};
            SaxCountdown scp(0);
            CHECK(!sax_parse(v, &scp));
        }

        SECTION("start_object(len)") {
            std::vector<uint8_t> const v = {0x81, 0xa3, 0x66, 0x6F, 0x6F, 0xc2};
            SaxCountdown scp(0);
            CHECK(!sax_parse(v, &scp));
        }

        SECTION("key()") {
            std::vector<uint8_t> const v = {0x81, 0xa3, 0x66, 0x6F, 0x6F, 0xc2};
            SaxCountdown scp(1);
            CHECK(!sax_parse(v, &scp));
        }
    }
}*/