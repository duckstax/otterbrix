#include <catch2/catch.hpp>

#include <core/assert/trace_full_exception.hpp>
#include <core/buffer.hpp>
#include <core/uvector.hpp>

#include <dataframe/bitmask.hpp>
#include <dataframe/detail/bitmask.hpp>
#include <dataframe/types.hpp>

#include <dataframe/tests/tools.hpp>

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>

#ifdef NDEBUG
#define IF_REQUIRE_THROWS_AS(f, error) REQUIRE_THROWS_AS(f, error)
#else
#define IF_REQUIRE_THROWS_AS(f, error)
#endif

using namespace components::dataframe;

TEST_CASE("state null count") {
    REQUIRE(0 == state_null_count(mask_state::unallocated, 42));
    REQUIRE(unknown_null_count == state_null_count(mask_state::uninitialized, 42));
    REQUIRE(42 == state_null_count(mask_state::all_null, 42));
    REQUIRE(0 == state_null_count(mask_state::all_valid, 42));
}

TEST_CASE("bitmask allocation size") {
    REQUIRE(0u == bitmask_allocation_size_bytes(0));
    REQUIRE(64u == bitmask_allocation_size_bytes(1));
    REQUIRE(64u == bitmask_allocation_size_bytes(512));
    REQUIRE(128u == bitmask_allocation_size_bytes(513));
    REQUIRE(128u == bitmask_allocation_size_bytes(1023));
    REQUIRE(128u == bitmask_allocation_size_bytes(1024));
    REQUIRE(192u == bitmask_allocation_size_bytes(1025));
}

TEST_CASE("num bitmask words") {
    REQUIRE(0 == num_bitmask_words(0));
    REQUIRE(1 == num_bitmask_words(1));
    REQUIRE(1 == num_bitmask_words(31));
    REQUIRE(1 == num_bitmask_words(32));
    REQUIRE(2 == num_bitmask_words(33));
    REQUIRE(2 == num_bitmask_words(63));
    REQUIRE(2 == num_bitmask_words(64));
    REQUIRE(3 == num_bitmask_words(65));
}

TEST_CASE("null mask") {
    auto resource = std::pmr::synchronized_pool_resource();
    IF_REQUIRE_THROWS_AS(detail::count_set_bits(&resource, nullptr, 0, 32), core::trace_full_exception);
    REQUIRE(32 == test::valid_count(&resource, nullptr, 0, 32));

    std::vector<size_type> indices = {0, 32, 7, 25};
    IF_REQUIRE_THROWS_AS(detail::segmented_count_set_bits(&resource, nullptr, indices), core::trace_full_exception);
    auto valid_counts = detail::segmented_valid_count(&resource, nullptr, indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({32, 18})));
}

core::uvector<bitmask_type> make_mask(std::pmr::memory_resource* resource, size_type size, bool fill_valid = false) {
    if (!fill_valid) {
        return core::make_empty_uvector<bitmask_type>(resource, size_t(size));
    } else {
        auto ret = core::uvector<bitmask_type>(resource, size_t(size));
        std::memset(ret.data(), -1, size_t(size) * sizeof(bitmask_type));
        return ret;
    }
}

TEST_CASE("negative start") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    IF_REQUIRE_THROWS_AS(detail::count_set_bits(&resource, mask.data(), -1, 32), core::trace_full_exception);
    IF_REQUIRE_THROWS_AS(test::valid_count(&resource, mask.data(), -1, 32), core::trace_full_exception);

    std::vector<size_type> indices = {0, 16, -1, 32};
    IF_REQUIRE_THROWS_AS(detail::segmented_count_set_bits(&resource, mask.data(), indices), core::trace_full_exception);
    IF_REQUIRE_THROWS_AS(detail::segmented_valid_count(&resource, mask.data(), indices), core::trace_full_exception);
}

TEST_CASE("start larger than stop") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    IF_REQUIRE_THROWS_AS(detail::count_set_bits(&resource, mask.data(), 32, 31), core::trace_full_exception);
    IF_REQUIRE_THROWS_AS(test::valid_count(&resource, mask.data(), 32, 31), core::trace_full_exception);

    std::vector<size_type> indices = {0, 16, 31, 30};
    IF_REQUIRE_THROWS_AS(detail::segmented_count_set_bits(&resource, mask.data(), indices), core::trace_full_exception);
    IF_REQUIRE_THROWS_AS(detail::segmented_valid_count(&resource, mask.data(), indices), core::trace_full_exception);
}

TEST_CASE("empty range") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(0 == detail::count_set_bits(&resource, mask.data(), 17, 17));
    REQUIRE(0 == test::valid_count(&resource, mask.data(), 17, 17));

    std::vector<size_type> indices = {0, 0, 17, 17};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
}

TEST_CASE("single word all zero") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(0 == detail::count_set_bits(&resource, mask.data(), 0, 32));
    REQUIRE(0 == test::valid_count(&resource, mask.data(), 0, 32));

    std::vector<size_type> indices = {0, 32, 0, 32};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
    auto valid_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
}

TEST_CASE("single bit all zero") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(0 == detail::count_set_bits(&resource, mask.data(), 17, 18));
    REQUIRE(0 == test::valid_count(&resource, mask.data(), 17, 18));

    std::vector<size_type> indices = {17, 18, 7, 8};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
}

TEST_CASE("single bit all set") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(1 == detail::count_set_bits(&resource, mask.data(), 13, 14));
    REQUIRE(1 == test::valid_count(&resource, mask.data(), 13, 14));

    std::vector<size_type> indices = {13, 14, 0, 1};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({1, 1})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({1, 1})));
}

TEST_CASE("single word all bits set") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(32 == detail::count_set_bits(&resource, mask.data(), 0, 32));
    REQUIRE(32 == test::valid_count(&resource, mask.data(), 0, 32));

    std::vector<size_type> indices = {0, 32, 0, 32};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({32, 32})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({32, 32})));
}

TEST_CASE("single word pre slack") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(25 == detail::count_set_bits(&resource, mask.data(), 7, 32));
    REQUIRE(25 == test::valid_count(&resource, mask.data(), 7, 32));

    std::vector<size_type> indices = {7, 32, 8, 32};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({25, 24})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({25, 24})));
}

TEST_CASE("single word post slack") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(17 == detail::count_set_bits(&resource, mask.data(), 0, 17));
    REQUIRE(17 == test::valid_count(&resource, mask.data(), 0, 17));

    std::vector<size_type> indices = {0, 17, 0, 18};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({17, 18})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({17, 18})));
}

TEST_CASE("single word subset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(30 == detail::count_set_bits(&resource, mask.data(), 1, 31));
    REQUIRE(30 == test::valid_count(&resource, mask.data(), 1, 31));

    std::vector<size_type> indices = {1, 31, 7, 17};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({30, 10})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({30, 10})));
}

TEST_CASE("single word subset2") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(28 == detail::count_set_bits(&resource, mask.data(), 2, 30));
    REQUIRE(28 == test::valid_count(&resource, mask.data(), 2, 30));

    std::vector<size_type> indices = {4, 16, 2, 30};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({12, 28})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({12, 28})));
}

TEST_CASE("multiple words all bits") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10, true);
    REQUIRE(320 == detail::count_set_bits(&resource, mask.data(), 0, 320));
    REQUIRE(320 == test::valid_count(&resource, mask.data(), 0, 320));

    std::vector<size_type> indices = {0, 320, 0, 320};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({320, 320})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({320, 320})));
}

TEST_CASE("multiple words subset word boundary") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10, true);
    REQUIRE(256 == detail::count_set_bits(&resource, mask.data(), 32, 288));
    REQUIRE(256 == test::valid_count(&resource, mask.data(), 32, 288));

    std::vector<size_type> indices = {32, 192, 32, 288};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({160, 256})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({160, 256})));
}

TEST_CASE("multiple words split word boundary") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10, true);
    REQUIRE(2 == detail::count_set_bits(&resource, mask.data(), 31, 33));
    REQUIRE(2 == test::valid_count(&resource, mask.data(), 31, 33));

    std::vector<size_type> indices = {31, 33, 60, 67};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({2, 7})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({2, 7})));
}

TEST_CASE("multiple words subset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10, true);
    REQUIRE(226 == detail::count_set_bits(&resource, mask.data(), 67, 293));
    REQUIRE(226 == test::valid_count(&resource, mask.data(), 67, 293));

    std::vector<size_type> indices = {67, 293, 37, 319};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({226, 282})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({226, 282})));
}

TEST_CASE("multiple words single bit") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10, true);
    REQUIRE(1 == detail::count_set_bits(&resource, mask.data(), 67, 68));
    REQUIRE(1 == test::valid_count(&resource, mask.data(), 67, 68));

    std::vector<size_type> indices = {67, 68, 31, 32, 192, 193};
    auto set_counts = detail::segmented_count_set_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(set_counts, Catch::Equals(std::pmr::vector<size_type>({1, 1, 1})));
    auto valid_counts = detail::segmented_valid_count(&resource, mask.data(), indices);
    REQUIRE_THAT(valid_counts, Catch::Equals(std::pmr::vector<size_type>({1, 1, 1})));
}

TEST_CASE("single bit all set 0") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1, true);
    REQUIRE(0 == detail::count_unset_bits(&resource, mask.data(), 13, 14));
    REQUIRE(0 == detail::null_count(&resource, mask.data(), 13, 14));

    std::vector<size_type> indices = {13, 14, 31, 32};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
    auto null_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
}

TEST_CASE("null mask 0") {
    auto resource = std::pmr::synchronized_pool_resource();
    IF_REQUIRE_THROWS_AS(detail::count_unset_bits(&resource, nullptr, 0, 32), core::trace_full_exception);
    REQUIRE(0 == detail::null_count(&resource, nullptr, 0, 32));

    std::vector<size_type> indices = {0, 32, 7, 25};
    IF_REQUIRE_THROWS_AS(detail::segmented_count_unset_bits(&resource, nullptr, indices), core::trace_full_exception);
    auto null_counts = detail::segmented_null_count(&resource, nullptr, indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({0, 0})));
}

TEST_CASE("single word all bits") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(32 == detail::count_unset_bits(&resource, mask.data(), 0, 32));
    REQUIRE(32 == detail::null_count(&resource, mask.data(), 0, 32));

    std::vector<size_type> indices = {0, 32, 0, 32};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({32, 32})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({32, 32})));
}

TEST_CASE("single word pre slack unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(25 == detail::count_unset_bits(&resource, mask.data(), 7, 32));
    REQUIRE(25 == detail::null_count(&resource, mask.data(), 7, 32));

    std::vector<size_type> indices = {7, 32, 8, 32};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({25, 24})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({25, 24})));
}

TEST_CASE("single word post slack unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(17 == detail::count_unset_bits(&resource, mask.data(), 0, 17));
    REQUIRE(17 == detail::null_count(&resource, mask.data(), 0, 17));

    std::vector<size_type> indices = {0, 17, 0, 18};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({17, 18})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({17, 18})));
}

TEST_CASE("single word subset unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(30 == detail::count_unset_bits(&resource, mask.data(), 1, 31));
    REQUIRE(30 == detail::null_count(&resource, mask.data(), 1, 31));

    std::vector<size_type> indices = {1, 31, 7, 17};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({30, 10})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({30, 10})));
}

TEST_CASE("single word subset2 unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    REQUIRE(28 == detail::count_unset_bits(&resource, mask.data(), 2, 30));
    REQUIRE(28 == detail::null_count(&resource, mask.data(), 2, 30));

    std::vector<size_type> indices = {4, 16, 2, 30};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({12, 28})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({12, 28})));
}

TEST_CASE("multiple words all bits unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10);
    REQUIRE(320 == detail::count_unset_bits(&resource, mask.data(), 0, 320));
    REQUIRE(320 == detail::null_count(&resource, mask.data(), 0, 320));

    std::vector<size_type> indices = {0, 320, 0, 320};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({320, 320})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({320, 320})));
}

TEST_CASE("multiple words subset word boundary unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10);
    REQUIRE(256 == detail::count_unset_bits(&resource, mask.data(), 32, 288));
    REQUIRE(256 == detail::null_count(&resource, mask.data(), 32, 288));

    std::vector<size_type> indices = {32, 192, 32, 288};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({160, 256})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({160, 256})));
}

TEST_CASE("multiple words split word boundary unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10);
    REQUIRE(2 == detail::count_unset_bits(&resource, mask.data(), 31, 33));
    REQUIRE(2 == detail::null_count(&resource, mask.data(), 31, 33));

    std::vector<size_type> indices = {31, 33, 60, 67};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({2, 7})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({2, 7})));
}

TEST_CASE("multiple words subset unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10);
    REQUIRE(226 == detail::count_unset_bits(&resource, mask.data(), 67, 293));
    REQUIRE(226 == detail::null_count(&resource, mask.data(), 67, 293));

    std::vector<size_type> indices = {67, 293, 37, 319};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({226, 282})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({226, 282})));
}

TEST_CASE("multiple words single bit unset") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 10);
    REQUIRE(1 == detail::count_unset_bits(&resource, mask.data(), 67, 68));
    REQUIRE(1 == detail::null_count(&resource, mask.data(), 67, 68));

    std::vector<size_type> indices = {67, 68, 31, 32, 192, 193};
    auto unset_counts = detail::segmented_count_unset_bits(&resource, mask.data(), indices);
    REQUIRE_THAT(unset_counts, Catch::Equals(std::pmr::vector<size_type>({1, 1, 1})));
    auto null_counts = detail::segmented_null_count(&resource, mask.data(), indices);
    REQUIRE_THAT(null_counts, Catch::Equals(std::pmr::vector<size_type>({1, 1, 1})));
}

void clean_end_word(core::buffer& mask, int begin_bit, int end_bit) {
    auto ptr = static_cast<bitmask_type*>(mask.data());

    auto number_of_mask_words = num_bitmask_words(static_cast<size_type>(end_bit - begin_bit));
    auto number_of_bits = end_bit - begin_bit;
    if (number_of_bits % 32 != 0) {
        bitmask_type end_mask = 0;
        std::memcpy(&end_mask, ptr + number_of_mask_words - 1, sizeof(end_mask));
        end_mask = end_mask & ((1 << (number_of_bits % 32)) - 1);
        std::memcpy(ptr + number_of_mask_words - 1, &end_mask, sizeof(end_mask));
    }
}

TEST_CASE("negative start 2") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    IF_REQUIRE_THROWS_AS(copy_bitmask(&resource, mask.data(), -1, 32), core::trace_full_exception);
}

TEST_CASE("start larger than stop 2") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    IF_REQUIRE_THROWS_AS(copy_bitmask(&resource, mask.data(), 32, 31), core::trace_full_exception);
}

TEST_CASE("empty range 2") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto mask = make_mask(&resource, 1);
    auto buff = copy_bitmask(&resource, mask.data(), 17, 17);
    REQUIRE(0 == static_cast<int>(buff.size()));
}

TEST_CASE("null_ptr") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto buff = copy_bitmask(&resource, nullptr, 17, 17);
    REQUIRE(0 == static_cast<int>(buff.size()));
}

TEST_CASE("test zero offset") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::vector<int> validity_bit(1000);
    for (auto& m : validity_bit) {
        m = GENERATE(0, 1); /// uniform random generator int {0, 1}
    }
    auto input_mask = test::make_null_mask(&resource, validity_bit.begin(), validity_bit.end());

    int begin_bit = 0;
    int end_bit = 800;
    auto gold_splice_mask =
        test::make_null_mask(&resource, validity_bit.begin() + begin_bit, validity_bit.begin() + end_bit);
    auto splice_mask = copy_bitmask(&resource, static_cast<const bitmask_type*>(input_mask.data()), begin_bit, end_bit);

    clean_end_word(splice_mask, begin_bit, end_bit);
    auto number_of_bits = end_bit - begin_bit;
    REQUIRE(test::equal(gold_splice_mask.data(), splice_mask.data(), size_t(num_bitmask_words(number_of_bits))));
}

TEST_CASE("test non zero offset") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::vector<int> validity_bit(1000);
    for (auto& m : validity_bit) {
        m = GENERATE(0, 1); /// uniform random generator int {0, 1}
    }
    auto input_mask = test::make_null_mask(&resource, validity_bit.begin(), validity_bit.end());

    int begin_bit = 321;
    int end_bit = 998;
    auto gold_splice_mask =
        test::make_null_mask(&resource, validity_bit.begin() + begin_bit, validity_bit.begin() + end_bit);

    auto splice_mask = copy_bitmask(&resource, static_cast<const bitmask_type*>(input_mask.data()), begin_bit, end_bit);

    clean_end_word(splice_mask, begin_bit, end_bit);
    auto number_of_bits = end_bit - begin_bit;
    REQUIRE(test::equal(gold_splice_mask.data(), splice_mask.data(), size_t(num_bitmask_words(number_of_bits))));
}

std::ostream& operator<<(std::ostream& stream, std::pmr::vector<bool> const& bits) {
    for (auto _bit : bits) stream << int(_bit);
    return stream;
}

void expect_bitmask_equal(std::pmr::memory_resource* resource,
                          bitmask_type const* bitmask, // Device &resource
                          size_type start_bit,
                          std::pmr::vector<bool> const& expect) {
    core::uvector<bool> result(resource, expect.size());
    auto counting_iter = boost::iterators::make_counting_iterator<size_type>(0);
    std::transform(counting_iter + start_bit,
                   counting_iter + start_bit + size_type(expect.size()),
                   result.begin(),
                   [bitmask](size_type element_index) { return detail::is_set_bit(bitmask, element_index); });
    auto vector_result = core::make_vector(resource, result);
    REQUIRE_THAT(vector_result, Catch::Equals(expect));
}

void test_set_null_range(std::pmr::memory_resource* resource,
                         size_type size,
                         size_type begin,
                         size_type end,
                         bool valid) {
    std::pmr::vector<bool> expected(size_t(end - begin), valid);
    core::buffer mask = create_null_mask(resource, size, mask_state::uninitialized);
    set_null_mask(static_cast<bitmask_type*>(mask.data()), begin, end, valid);
    expect_bitmask_equal(resource, static_cast<bitmask_type*>(mask.data()), begin, expected);
}

void test_null_partition(std::pmr::memory_resource* resource, size_type size, size_type middle, bool valid) {
    std::pmr::vector<bool> expected(static_cast<size_t>(size));
    std::generate(expected.begin(), expected.end(), [n = 0, middle, valid]() mutable {
        auto i = n++;
        return (!valid) ^ (i < middle);
    });

    core::buffer mask = create_null_mask(resource, size, mask_state::uninitialized);
    set_null_mask(static_cast<bitmask_type*>(mask.data()), 0, middle, valid);
    set_null_mask(static_cast<bitmask_type*>(mask.data()), middle, size, !valid);
    expect_bitmask_equal(resource, static_cast<bitmask_type*>(mask.data()), 0, expected);
}

TEST_CASE("fill_range") {
    auto resource = std::pmr::synchronized_pool_resource();
    size_type size = 121;
    for (auto begin = 0; begin < size; begin += 5)
        for (auto end = begin + 1; end <= size; end += 7) {
            test_set_null_range(&resource, size, begin, end, true);
            test_set_null_range(&resource, size, begin, end, false);
        }
}

TEST_CASE("null_mask_partition") {
    auto resource = std::pmr::synchronized_pool_resource();
    size_type size = 64;
    for (auto middle = 1; middle < size; middle++) {
        test_null_partition(&resource, size, middle, true);
        test_null_partition(&resource, size, middle, false);
    }
}

TEST_CASE("error_range") {
    auto resource = std::pmr::synchronized_pool_resource();
    size_type size = 121;
    using size_pair = std::pair<size_type, size_type>;
    std::vector<size_pair> begin_end_fail{
        {-1, size}, // begin>=0
        {-2, -1},   // begin>=0
        {9, 8},     // begin<=end
    };
    for (auto begin_end : begin_end_fail) {
        auto begin = begin_end.first, end = begin_end.second;
        IF_REQUIRE_THROWS_AS(test_set_null_range(&resource, size, begin, end, true), core::trace_full_exception);
        IF_REQUIRE_THROWS_AS(test_set_null_range(&resource, size, begin, end, false), core::trace_full_exception);
    }
    std::vector<size_pair> begin_end_pass{
        {0, size},        // begin>=0
        {0, 1},           // begin>=0
        {8, 8},           // begin==end
        {8, 9},           // begin<=end
        {size - 1, size}, // begin<=end
    };
    for (auto begin_end : begin_end_pass) {
        auto begin = begin_end.first, end = begin_end.second;
        REQUIRE_NOTHROW(test_set_null_range(&resource, size, begin, end, true));
        REQUIRE_NOTHROW(test_set_null_range(&resource, size, begin, end, false));
    }
}