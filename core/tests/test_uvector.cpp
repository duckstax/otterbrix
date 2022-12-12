#include "core/uvector.hpp"
#include <catch2/catch.hpp>

#include <cstddef>
#include <memory_resource>

TEMPLATE_TEST_CASE("MemoryResource", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    std::pmr::memory_resource* resource = std::pmr::get_default_resource();
    core::uvector<TestType> vec(resource, 128);
    REQUIRE(vec.memory_resource() == std::pmr::get_default_resource());
}

TEMPLATE_TEST_CASE("ZeroSizeConstructor", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, 0);
    REQUIRE(vec.size() == 0);
    REQUIRE(vec.end() == vec.begin());
    REQUIRE(vec.is_empty());
}

TEMPLATE_TEST_CASE("NonZeroSizeConstructor", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    REQUIRE(vec.size() == size);
    REQUIRE(vec.ssize() == size);
    REQUIRE(vec.data() != nullptr);
    REQUIRE(vec.end() == vec.begin() + vec.size());
    REQUIRE(!vec.is_empty());
    REQUIRE(vec.element_ptr(0) != nullptr);
}

TEMPLATE_TEST_CASE("CopyConstructor", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    auto const size{12345};
    core::uvector<TestType> vec(mr, size);
    core::uvector<TestType> uv_copy(mr, vec);
    REQUIRE(uv_copy.size() == vec.size());
    REQUIRE(uv_copy.data() != nullptr);
    REQUIRE(uv_copy.end() == uv_copy.begin() + uv_copy.size());
    REQUIRE(!uv_copy.is_empty());
    REQUIRE(uv_copy.element_ptr(0) != nullptr);
}

TEMPLATE_TEST_CASE("ResizeSmaller", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const original_size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, original_size);
    auto* original_data = vec.data();
    auto* original_begin = vec.begin();

    auto smaller_size = vec.size() - 1;
    vec.resize(smaller_size);

    REQUIRE(original_data == vec.data());
    REQUIRE(original_begin == vec.begin());
    REQUIRE(vec.size() == smaller_size);
    REQUIRE(vec.capacity() == original_size);

    vec.shrink_to_fit();
    REQUIRE(vec.size() == smaller_size);
    REQUIRE(vec.capacity() == smaller_size);
}

TEMPLATE_TEST_CASE("ResizeLarger", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const original_size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, original_size);
    auto* original_data = vec.data();
    auto* original_begin = vec.begin();

    auto larger_size = vec.size() + 1;
    vec.resize(larger_size);

    REQUIRE(vec.data() != original_data);
    REQUIRE(vec.begin() != original_begin);
    REQUIRE(vec.size() == larger_size);
    REQUIRE(vec.capacity() == larger_size);

    auto* larger_data = vec.data();
    auto* larger_begin = vec.begin();

    vec.shrink_to_fit();
    REQUIRE(vec.size() == larger_size);
    REQUIRE(vec.capacity() == larger_size);
    REQUIRE(vec.data() == larger_data);
    REQUIRE(vec.begin() == larger_begin);
}

TEMPLATE_TEST_CASE("ReserveSmaller", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const original_size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, original_size);
    auto* const original_data = vec.data();
    auto* const original_begin = vec.begin();
    auto const original_capacity = vec.capacity();

    auto const smaller_capacity = vec.capacity() - 1;
    vec.reserve(smaller_capacity);

    REQUIRE(vec.data() == original_data);
    REQUIRE(vec.begin() == original_begin);
    REQUIRE(vec.size() == original_size);
    REQUIRE(vec.capacity() == original_capacity);
}

TEMPLATE_TEST_CASE("ReserveLarger", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const original_size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, original_size);
    vec.set_element(0, 1);
    auto* const original_data = vec.data();
    auto* const original_begin = vec.begin();

    auto const larger_capacity = vec.capacity() + 1;
    vec.reserve(larger_capacity);

    REQUIRE(vec.data() != original_data);
    REQUIRE(vec.begin() != original_begin);
    REQUIRE(vec.size() == original_size);
    REQUIRE(vec.capacity() == larger_capacity);
    REQUIRE(vec.element(0) == 1);
}

TEMPLATE_TEST_CASE("ResizeToZero", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const original_size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, original_size);
    vec.resize(0);

    REQUIRE(vec.size() == 0);
    REQUIRE(vec.is_empty());
    REQUIRE(vec.capacity() == original_size);

    vec.shrink_to_fit();
    REQUIRE(vec.capacity() == 0);
}

TEMPLATE_TEST_CASE("Release", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const original_size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, original_size);

    auto* original_data = vec.data();

    core::buffer storage = vec.release();

    REQUIRE(vec.size() == 0);
    REQUIRE(vec.capacity() == 0);
    REQUIRE(vec.is_empty());
    REQUIRE(storage.data() == original_data);
    REQUIRE(storage.size() == original_size * sizeof(TestType));
}

TEMPLATE_TEST_CASE("ElementPointer", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    for (std::size_t i = 0; i < vec.size(); ++i) {
        REQUIRE(vec.element_ptr(i) != nullptr);
    }
}

/*

TEMPLATE_TEST_CASE("OOBSetElement", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    vec.set_element(vec.size() + 1, 42);
    ///REQUIRE(vec.set_element(vec.size() + 1, 42));
}

TEMPLATE_TEST_CASE("OOBGetElement", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    auto foo = [&]() { return vec.element(vec.size() + 1); };
    REQUIRE(foo());
}
 */

TEMPLATE_TEST_CASE("GetSetElement", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    for (std::size_t i = 0; i < vec.size(); ++i) {
        vec.set_element(i, i);
        REQUIRE(static_cast<TestType>(i) == vec.element(i));
    }
}

TEMPLATE_TEST_CASE("GetSetElementAsync", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    for (std::size_t i = 0; i < vec.size(); ++i) {
        auto init = static_cast<TestType>(i);
        vec.set_element_async(i, init);
        REQUIRE(init == vec.element(i));
    }
}

TEMPLATE_TEST_CASE("SetElementZeroAsync", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);
    for (std::size_t i = 0; i < vec.size(); ++i) {
        vec.set_element_to_zero_async(i);
        REQUIRE(TestType{0} == vec.element(i));
    }
}

TEMPLATE_TEST_CASE("FrontBackElement", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);

    auto const first = TestType{42};
    auto const last = TestType{13};
    vec.set_element(0, first);
    vec.set_element(vec.size() - 1, last);

    REQUIRE(first == vec.front_element());
    REQUIRE(last == vec.back_element());
}

TEMPLATE_TEST_CASE("Iterators", "[vector][template]", std::int8_t, std::int32_t, std::uint64_t, float, double) {
    auto const size{12345};
    std::pmr::memory_resource* mr = std::pmr::get_default_resource();
    core::uvector<TestType> vec(mr, size);

    REQUIRE(vec.begin() == vec.data());
    REQUIRE(vec.cbegin() == vec.data());

    auto const* const_begin = std::as_const(vec).begin();
    REQUIRE(const_begin == vec.cbegin());

    REQUIRE(std::distance(vec.begin(), vec.end()) == vec.size());
    REQUIRE(std::distance(vec.cbegin(), vec.cend()) == vec.size());

    auto const* const_end = std::as_const(vec).end();
    REQUIRE(const_end == vec.cend());
}