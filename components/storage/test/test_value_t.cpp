#include <boost/test/unit_test.hpp>
#include "value.hpp"
#include "pointer.hpp"
#include "varint.hpp"
#include "deep_iterator.hpp"
#include "shared_keys.hpp"
#include "doc.hpp"
#include <iostream>
#include <sstream>

using namespace storage;

BOOST_AUTO_TEST_SUITE(value_t)

BOOST_AUTO_TEST_CASE(var_int_read) {
    uint8_t buf[100];
    uint64_t result;
    double d = 0.0;
    while (d <= static_cast<double>(UINT64_MAX)) {
        auto n = static_cast<uint64_t>(d);
        auto nBytes = put_uvar_int(buf, n);
        BOOST_CHECK(get_uvar_int(slice_t(buf, sizeof(buf)), &result) == nBytes);
        BOOST_CHECK(result == n);
        BOOST_CHECK(get_uvar_int(slice_t(buf, nBytes), &result) == nBytes);
        BOOST_CHECK(result == n);
        BOOST_CHECK(get_uvar_int(slice_t(buf, nBytes - 1), &result) == 0);
        d = std::max(d, 1.0) * 1.5;
    }
    memset(buf, 0x88, sizeof(buf));
    BOOST_CHECK(get_uvar_int(slice_t(buf, sizeof(buf)), &result) == 0);
}

BOOST_AUTO_TEST_CASE(var_int32_read) {
    uint8_t buf[100];
    double d = 0.0;
    while (d <= static_cast<double>(UINT64_MAX)) {
        auto n = static_cast<uint64_t>(d);
        auto nBytes = put_uvar_int(buf, n);
        uint32_t result;
        if (n <= UINT32_MAX) {
            BOOST_CHECK(get_uvar_int32(slice_t(buf, sizeof(buf)), &result) == nBytes);
            BOOST_CHECK(result == n);
            BOOST_CHECK(get_uvar_int32(slice_t(buf, nBytes), &result) == nBytes);
            BOOST_CHECK(result == n);
            BOOST_CHECK(get_uvar_int32(slice_t(buf, nBytes - 1), &result) == 0);
        } else {
            BOOST_CHECK(get_uvar_int32(slice_t(buf, sizeof(buf)), &result) == 0);
        }
        d = std::max(d, 1.0) * 1.5;
    }
}

BOOST_AUTO_TEST_CASE(constants) {
    BOOST_CHECK(impl::value_t::null_value->type() == impl::value_type::null);
    BOOST_CHECK(!impl::value_t::null_value->is_undefined());
    BOOST_CHECK(!impl::value_t::null_value->is_mutable());

    BOOST_CHECK(impl::value_t::undefined_value->type() == impl::value_type::null);
    BOOST_CHECK(impl::value_t::undefined_value->is_undefined());
    BOOST_CHECK(!impl::value_t::undefined_value->is_mutable());

    BOOST_CHECK(impl::array_t::empty_array->type() == impl::value_type::array);
    BOOST_CHECK(impl::array_t::empty_array->count() == 0);
    BOOST_CHECK(!impl::array_t::empty_array->is_mutable());

    BOOST_CHECK(impl::dict_t::empty_dict->type() == impl::value_type::dict);
    BOOST_CHECK(impl::dict_t::empty_dict->count() == 0);
    BOOST_CHECK(!impl::dict_t::empty_dict->is_mutable());
}

BOOST_AUTO_TEST_CASE(pointers) {
    impl::internal::pointer_t v(4, impl::internal::size_narrow);
    BOOST_CHECK(v.offset<false>() == 4u);
    impl::internal::pointer_t w(4, impl::internal::size_wide);
    BOOST_CHECK(w.offset<true>() == 4u);
}

BOOST_AUTO_TEST_CASE(deref) {
    uint8_t data[10] = {0x01, 0x02, 0x03, 0x04, 0x80, 0x02};
    auto start = reinterpret_cast<const impl::internal::pointer_t*>(&data[4]);
    BOOST_CHECK(start->offset<false>() == 4u);
    auto dst = start->deref<false>();
    BOOST_CHECK(reinterpret_cast<ptrdiff_t>(dst) - reinterpret_cast<ptrdiff_t>(&data[0]) == 0L);
}

BOOST_AUTO_TEST_SUITE_END()
