#include <catch2/catch.hpp>
#include <components/document/support/varint.hpp>
#include <components/document/core/pointer.hpp>
#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>

using namespace document;
using document::impl::value_type;

TEST_CASE("value_t") {

    SECTION("Var int read") {
        uint8_t buf[100];
        uint64_t result;
        double d = 0.0;
        while (d <= static_cast<double>(UINT64_MAX)) {
            auto n = static_cast<uint64_t>(d);
            auto nBytes = put_uvar_int(buf, n);
            REQUIRE(get_uvar_int(std::string_view(reinterpret_cast<char*>(buf), sizeof(buf)), &result) == nBytes);
            REQUIRE(result == n);
            REQUIRE(get_uvar_int(std::string_view(reinterpret_cast<char*>(buf), nBytes), &result) == nBytes);
            REQUIRE(result == n);
            REQUIRE(get_uvar_int(std::string_view(reinterpret_cast<char*>(buf), nBytes - 1), &result) == 0);
            d = std::max(d, 1.0) * 1.5;
        }
        memset(buf, 0x88, sizeof(buf));
        REQUIRE(get_uvar_int(std::string_view(reinterpret_cast<char*>(buf), sizeof(buf)), &result) == 0);
    }

    SECTION("Var int32 read") {
        uint8_t buf[100];
        double d = 0.0;
        while (d <= static_cast<double>(UINT64_MAX)) {
            auto n = static_cast<uint64_t>(d);
            auto nBytes = put_uvar_int(buf, n);
            uint32_t result;
            if (n <= UINT32_MAX) {
                REQUIRE(get_uvar_int32(std::string_view(reinterpret_cast<char*>(buf), sizeof(buf)), &result) == nBytes);
                REQUIRE(result == n);
                REQUIRE(get_uvar_int32(std::string_view(reinterpret_cast<char*>(buf), nBytes), &result) == nBytes);
                REQUIRE(result == n);
                REQUIRE(get_uvar_int32(std::string_view(reinterpret_cast<char*>(buf), nBytes - 1), &result) == 0);
            } else {
                REQUIRE(get_uvar_int32(std::string_view(reinterpret_cast<char*>(buf), sizeof(buf)), &result) == 0);
            }
            d = std::max(d, 1.0) * 1.5;
        }
    }

    SECTION("Constants") {
        REQUIRE(impl::value_t::null_value->type() == value_type::null);
        REQUIRE_FALSE(impl::value_t::null_value->is_undefined());
        REQUIRE_FALSE(impl::value_t::null_value->is_mutable());

        REQUIRE(impl::value_t::undefined_value->type() == value_type::undefined);
        REQUIRE(impl::value_t::undefined_value->is_undefined());
        REQUIRE_FALSE(impl::value_t::undefined_value->is_mutable());

        REQUIRE(impl::array_t::empty_array()->type() == value_type::array);
        REQUIRE(impl::array_t::empty_array()->count() == 0);
        REQUIRE_FALSE(impl::array_t::empty_array()->is_mutable());

        REQUIRE(impl::dict_t::empty_dict()->type() == value_type::dict);
        REQUIRE(impl::dict_t::empty_dict()->count() == 0);
        REQUIRE_FALSE(impl::dict_t::empty_dict()->is_mutable());
    }

    SECTION("Pointers") {
        impl::internal::pointer_t v(4, impl::internal::size_narrow);
        REQUIRE(v.offset<false>() == 4u);
        impl::internal::pointer_t w(4, impl::internal::size_wide);
        REQUIRE(w.offset<true>() == 4u);
    }

    SECTION("Deref") {
        uint8_t data[10] = {0x01, 0x02, 0x03, 0x04, 0x80, 0x02};
        auto start = reinterpret_cast<const impl::internal::pointer_t*>(&data[4]);
        REQUIRE(start->offset<false>() == 4u);
        auto dst = start->deref<false>();
        REQUIRE(reinterpret_cast<ptrdiff_t>(dst) - reinterpret_cast<ptrdiff_t>(&data[0]) == 0L);
    }

}
