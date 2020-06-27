#include "gtest/gtest.h"
#include "components/buffer/mutable_buffer.hpp"
#include "components/buffer/const_buffer.hpp"
#include "components/buffer/type_traits.hpp"
#include <type_traits>
#include <array>

/*
static_assert(std::is_nothrow_swappable_v<const_buffer>, "");
static_assert(std::is_nothrow_swappable_v<mutable_buffer>, "");
static_assert(std::is_trivially_copyable_v<const_buffer>, "");
static_assert(std::is_trivially_copyable_v<mutable_buffer>, "");
*/
using namespace components;
TEST(buffer, buffer_default_ctor) {
    constexpr mutable_buffer mb;
    constexpr const_buffer cb;
    ASSERT_EQ(mb.size(), 0);
    ASSERT_EQ(mb.data(), nullptr);
    ASSERT_EQ(cb.size(), 0);
    ASSERT_EQ(cb.data(), nullptr);
}

TEST(buffer, buffer_data_ctor) {
    std::vector<int16_t> v(10);
    const_buffer cb(v.data(), v.size() * sizeof(int16_t));
    ASSERT_EQ(cb.size(), v.size() * sizeof(int16_t));
    ASSERT_EQ(cb.data(), v.data());
    mutable_buffer mb(v.data(), v.size() * sizeof(int16_t));
    ASSERT_EQ(mb.size(), v.size() * sizeof(int16_t));
    ASSERT_EQ(mb.data(), v.data());
    const_buffer from_mut = mb;
    ASSERT_EQ(mb.size(), from_mut.size());
    ASSERT_EQ(mb.data(), from_mut.data());
    const auto cmb = mb;
    static_assert(std::is_same<decltype(cmb.data()), void *>::value, "");

    constexpr void *p = nullptr;
    constexpr const_buffer cecb = buffer(p, 0);
    constexpr mutable_buffer cemb = buffer(p, 0);
    ASSERT_EQ(cecb.data(), nullptr);
    ASSERT_EQ(cemb.data(), nullptr);
}

TEST(buffer, const_buffer_operator_plus) {
    std::vector<int16_t> v(10);
    const_buffer cb(v.data(), v.size() * sizeof(int16_t));
    const size_t shift = 4;
    auto shifted = cb + shift;
    ASSERT_EQ(shifted.size(), v.size() * sizeof(int16_t) - shift);
    ASSERT_EQ(shifted.data(), v.data() + shift / sizeof(int16_t));
    auto shifted2 = shift + cb;
    ASSERT_EQ(shifted.size(), shifted2.size());
    ASSERT_EQ(shifted.data(), shifted2.data());
    auto cbinp = cb;
    cbinp += shift;
    ASSERT_EQ(shifted.size(), cbinp.size());
    ASSERT_EQ(shifted.data(), cbinp.data());
}

TEST(buffer, mutable_buffer_operator_plus) {
    std::vector<int16_t> v(10);
    mutable_buffer mb(v.data(), v.size() * sizeof(int16_t));
    const size_t shift = 4;
    auto shifted = mb + shift;
    ASSERT_EQ(shifted.size(), v.size() * sizeof(int16_t) - shift);
    ASSERT_EQ(shifted.data(), v.data() + shift / sizeof(int16_t));
    auto shifted2 = shift + mb;
    ASSERT_EQ(shifted.size(), shifted2.size());
    ASSERT_EQ(shifted.data(), shifted2.data());
    auto mbinp = mb;
    mbinp += shift;
    ASSERT_EQ(shifted.size(), mbinp.size());
    ASSERT_EQ(shifted.data(), mbinp.data());
}

TEST(buffer,mutable_buffer_creation_basic){
std::vector<int16_t> v(10);
mutable_buffer mb(v.data(), v.size() * sizeof(int16_t));
mutable_buffer mb2 = buffer(v.data(), v.size() * sizeof(int16_t));
ASSERT_EQ(mb.data(), mb2.data());
ASSERT_EQ(mb.size(), mb2.size());
mutable_buffer mb3 = buffer(mb);
ASSERT_EQ(mb.data(), mb3.data());
ASSERT_EQ(mb.size(), mb3.size());
mutable_buffer mb4 = buffer(mb, 10 * v.size() * sizeof(int16_t));
ASSERT_EQ(mb.data(), mb4.data());
ASSERT_EQ(mb.size(), mb4.size());
mutable_buffer mb5 = buffer(mb, 4);
ASSERT_EQ(mb.data(), mb5.data());
ASSERT_EQ(4, mb5.size());
}

TEST(buffer, const_buffer_creation_basic){
const std::vector<int16_t> v(10);
const_buffer cb(v.data(), v.size() * sizeof(int16_t));
const_buffer cb2 = buffer(v.data(), v.size() * sizeof(int16_t));
ASSERT_EQ(cb.data(), cb2.data());
ASSERT_EQ(cb.size(), cb2.size());
const_buffer cb3 = buffer(cb);
ASSERT_EQ(cb.data(), cb3.data());
ASSERT_EQ(cb.size(), cb3.size());
const_buffer cb4 = buffer(cb, 10 * v.size() * sizeof(int16_t));
ASSERT_EQ(cb.data(), cb4.data());
ASSERT_EQ(cb.size(), cb4.size());
const_buffer cb5 = buffer(cb, 4);
ASSERT_EQ(cb.data(), cb5.data());
ASSERT_EQ(4, cb5.size());
}

TEST(buffer,mutable_buffer_creation_C_array){
int16_t d[10] = {};
mutable_buffer b = buffer(d);
ASSERT_EQ(b.size(), 10 * sizeof(int16_t));
ASSERT_EQ(b.data(), static_cast<int16_t *>(d));
const_buffer b2 = buffer(d, 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), static_cast<int16_t *>(d));
}

TEST(buffer,const_buffer_creation_C_array){
const int16_t d[10] = {};
const_buffer b = buffer(d);
ASSERT_EQ(b.size(), 10 * sizeof(int16_t));
ASSERT_EQ(b.data(), static_cast<const int16_t *>(d));
const_buffer b2 = buffer(d, 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), static_cast<const int16_t *>(d));
}

TEST(buffer,mutable_buffer_creation_array){
std::array<int16_t, 10> d = {};
mutable_buffer b = buffer(d);
ASSERT_EQ(b.size(), d.size() * sizeof(int16_t));
ASSERT_EQ(b.data(), d.data());
mutable_buffer b2 = buffer(d, 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), d.data());
}


TEST(buffer,const_buffer_creation_array){
std::array<const int16_t, 10> d = {{}};
const_buffer b = buffer(d);
ASSERT_EQ(b.size(), d.size() * sizeof(int16_t));
ASSERT_EQ(b.data(), d.data());
const_buffer b2 = buffer(d, 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), d.data());
}

TEST(buffer,mutable_buffer_creation_vector ){
std::vector<int16_t> d(10);
mutable_buffer b = buffer(d);
ASSERT_EQ(b.size(), d.size() * sizeof(int16_t));
ASSERT_EQ(b.data(), d.data());
mutable_buffer b2 = buffer(d, 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), d.data());
d.clear();
b = buffer(d);
ASSERT_EQ(b.size(), 0);
ASSERT_EQ(b.data(), nullptr);
}

TEST(buffer,const_buffer_creation_vector ){
std::vector<int16_t> d(10);
const_buffer b = buffer(static_cast<const std::vector<int16_t> &>(d));
ASSERT_EQ(b.size(), d.size() * sizeof(int16_t));
ASSERT_EQ(b.data(), d.data());
const_buffer b2 = buffer(static_cast<const std::vector<int16_t> &>(d), 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), d.data());
d.clear();
b = buffer(static_cast<const std::vector<int16_t> &>(d));
ASSERT_EQ(b.size(), 0);
ASSERT_EQ(b.data(), nullptr);
}

TEST(buffer, const_buffer_creation_string) {
    const std::wstring d(10, L'a');
    const_buffer b = buffer(d);
    ASSERT_EQ(b.size(), d.size() * sizeof(wchar_t));
    ASSERT_EQ(b.data(), d.data());
    const_buffer b2 = buffer(d, 4);
    ASSERT_EQ(b2.size(), 4);
    ASSERT_EQ(b2.data(), d.data());
}

TEST(buffer, mutable_buffer_creation_string) {
    std::wstring d(10, L'a');
    mutable_buffer b = buffer(d);
    ASSERT_EQ(b.size(), d.size() * sizeof(wchar_t));
    ASSERT_EQ(b.data(), d.data());
    mutable_buffer b2 = buffer(d, 4);
    ASSERT_EQ(b2.size(), 4);
    ASSERT_EQ(b2.data(), d.data());
}


TEST(buffer,const_buffer_creation_string_view) {
std::wstring dstr(10, L'a');
boost::wstring_view d = dstr;
const_buffer b = buffer(d);
ASSERT_EQ(b.size(), d.size() * sizeof(wchar_t));
ASSERT_EQ(b.data(), d.data());
const_buffer b2 = buffer(d, 4);
ASSERT_EQ(b2.size(), 4);
ASSERT_EQ(b2.data(), d.data());
}

TEST(buffer,const_buffer_creation_with_str_buffer){
const wchar_t wd[10] = {};
const_buffer b = str_buffer(wd);
ASSERT_EQ(b.size(), 9 * sizeof(wchar_t));
ASSERT_EQ(b.data(), static_cast<const wchar_t *>(wd));

const_buffer b2_null = buffer("hello");
constexpr const_buffer b2 = str_buffer("hello");
ASSERT_EQ(b2_null.size(), 6);
ASSERT_EQ(b2.size(), 5);
ASSERT_EQ(std::string(static_cast<const char *>(b2.data()), b2.size()), "hello");
}

TEST(buffer,const_buffer_creation_with_buf_string_literal_char) {
using namespace literals;
constexpr const_buffer b = "hello"_buf;
ASSERT_EQ(b.size(), 5);
ASSERT_EQ(std::memcmp(b.data(), "hello", b.size()), 0);
}

TEST(buffer,const_buffer_creation_with_buf_string_literal_wchar_t) {
using namespace literals;
constexpr const_buffer b = L"hello"_buf;
ASSERT_EQ(b.size(), 5 * sizeof(wchar_t));
ASSERT_EQ(std::memcmp(b.data(), L"hello", b.size()), 0);
}

TEST(buffer, const_buffer_creation_with_buf_string_literal_char16_t) {
using namespace literals;
constexpr const_buffer b = u"hello"_buf;
ASSERT_EQ(b.size(), 5 * sizeof(char16_t));
ASSERT_EQ(std::memcmp(b.data(), u"hello", b.size()), 0);
}

TEST(buffer, const_buffer_creation_with_buf_string_literal_char32_t) {
    using namespace literals;
    constexpr const_buffer b = U"hello"_buf;
    ASSERT_EQ(b.size(), 5 * sizeof(char32_t));
    ASSERT_EQ(std::memcmp(b.data(), U"hello", b.size()), 0);
}

TEST(buffer, buffer_of_structs) {
    struct some_pod {
        int64_t val;
        char arr[8];
    };

    struct some_non_pod {
        int64_t val;
        char arr[8];
        std::vector<int> s; // not trivially copyable
    };
    static_assert(detail::is_pod_like<some_pod>::value, "");
    static_assert(!detail::is_pod_like<some_non_pod>::value, "");
    std::array<some_pod, 1> d{};
    mutable_buffer b = buffer(d);
    ASSERT_EQ(b.size(), d.size() * sizeof(some_pod));
    ASSERT_EQ(b.data(), d.data());
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}