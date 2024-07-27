#include <catch2/catch.hpp>

#include <algorithm>
#include <memory>
#include <memory_resource>
#include <random>

#include <core/assert/trace_full_exception.hpp>

#include "dataframe/bitmask.hpp"
#include "dataframe/column/column.hpp"
#include "dataframe/column/column_view.hpp"
#include "dataframe/column/make.hpp"
#include "dataframe/type_dispatcher.hpp"
#include "dataframe/types.hpp"

#include "dataframe/tests/tools.hpp"

using namespace components::dataframe;
using namespace components::dataframe::column;

#ifdef NDEBUG
#define IF_REQUIRE_THROWS_AS(f, error) REQUIRE_THROWS_AS(f, error)
#else
#define IF_REQUIRE_THROWS_AS(f, error)
#endif

namespace {

    bool equal(const void* lhs, const void* rhs, std::size_t size) {
        if (lhs == rhs || size == 0) {
            return true;
        }
        if (!lhs || !rhs) {
            return false;
        }
        return memcmp(lhs, rhs, size) == 0;
    }

    bool equal(const column_view& v1, const column_view& v2) { return equal(v1.head(), v2.head(), v1.size()); }

    bool equal(const column_t& c1, const column_t& c2) { return equal(c1.view(), c2.view()); }

} // namespace

template<typename T>
struct gen_column {
    data_type type() { return data_type{type_to_id<T>()}; }

    gen_column(std::pmr::memory_resource* resource)
        : resource_(resource)
        , data(resource_, _num_elements * size_of(type()))
        , mask(resource_, bitmask_allocation_size_bytes(_num_elements))
        , all_valid_mask(resource_, create_null_mask(resource_, num_elements(), mask_state::all_valid))
        , all_null_mask(resource_, create_null_mask(resource_, num_elements(), mask_state::all_null)) {
        test::sequence(data);
        test::sequence(mask);
    }

    size_type num_elements() { return _num_elements; }

    std::pmr::memory_resource* resource_;
    std::random_device r;
    std::default_random_engine generator{r()};
    std::uniform_int_distribution<size_type> distribution{200, 1000};
    size_type _num_elements{distribution(generator)};
    core::buffer data;
    core::buffer mask;
    core::buffer all_valid_mask;
    core::buffer all_null_mask;
};

void verify_column_views(column_t& col, std::pmr::memory_resource* resource) {
    column_view view = col;
    mutable_column_view mutable_view = col;
    REQUIRE(col.type() == view.type());
    REQUIRE(col.type() == mutable_view.type());
    REQUIRE(col.size() == view.size());
    REQUIRE(col.size() == mutable_view.size());
    REQUIRE(col.null_count() == view.null_count(resource));
    REQUIRE(col.null_count() == mutable_view.null_count(resource));
    REQUIRE(col.nullable() == view.nullable());
    REQUIRE(col.nullable() == mutable_view.nullable());
    REQUIRE(col.num_children() == view.num_children());
    REQUIRE(col.num_children() == mutable_view.num_children());
    REQUIRE(view.head() == mutable_view.head());
    REQUIRE(view.data<char>() == mutable_view.data<char>());
    REQUIRE(view.offset() == mutable_view.offset());
}

TEMPLATE_TEST_CASE("default null count no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    REQUIRE_FALSE(col.nullable());
    REQUIRE_FALSE(col.has_nulls());
    REQUIRE(0 == col.null_count());
}

TEMPLATE_TEST_CASE("default null count empty mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    REQUIRE_FALSE(col.nullable());
    REQUIRE_FALSE(col.has_nulls());
    REQUIRE(0 == col.null_count());
}

TEMPLATE_TEST_CASE("default null count all valid", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    REQUIRE(col.nullable());
    REQUIRE_FALSE(col.has_nulls());
    REQUIRE(0 == col.null_count());
}

TEMPLATE_TEST_CASE("explicit null count all valid", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask), 0};
    REQUIRE(col.nullable());
    REQUIRE_FALSE(col.has_nulls());
    REQUIRE(0 == col.null_count());
}

TEMPLATE_TEST_CASE("default null count all null", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_null_mask)};
    REQUIRE(col.nullable());
    REQUIRE(col.has_nulls());
    REQUIRE(gen.num_elements() == col.null_count());
}

TEMPLATE_TEST_CASE("explicit null count all null", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource,
                 gen.type(),
                 gen.num_elements(),
                 std::move(gen.data),
                 std::move(gen.all_null_mask),
                 gen.num_elements()};
    REQUIRE(col.nullable());
    REQUIRE(col.has_nulls());
    REQUIRE(gen.num_elements() == col.null_count());
}

TEMPLATE_TEST_CASE("set null count no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    IF_REQUIRE_THROWS_AS(col.set_null_count(1), core::trace_full_exception);
}

TEMPLATE_TEST_CASE("set empty null mask non zero null count", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    core::buffer empty_null_mask(&resource);
    IF_REQUIRE_THROWS_AS(col.set_null_mask(empty_null_mask, gen.num_elements()), core::trace_full_exception);
}

TEMPLATE_TEST_CASE("set invalid size null mask non zero null count", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    auto invalid_size_null_mask =
        create_null_mask(&resource, std::min(gen.num_elements() - 50, 0), mask_state::all_valid);
    IF_REQUIRE_THROWS_AS(col.set_null_mask(invalid_size_null_mask, gen.num_elements()), core::trace_full_exception);
}

TEMPLATE_TEST_CASE("set null count empty mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    IF_REQUIRE_THROWS_AS(col.set_null_count(1), core::trace_full_exception);
}

TEMPLATE_TEST_CASE("set null count all valid", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    REQUIRE_NOTHROW(col.set_null_count(0));
    REQUIRE(0 == col.null_count());
}

TEMPLATE_TEST_CASE("set null count all null", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_null_mask)};
    REQUIRE_NOTHROW(col.set_null_count(gen.num_elements()));
    REQUIRE(gen.num_elements() == col.null_count());
}

TEMPLATE_TEST_CASE("reset null count all null", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_null_mask)};
    REQUIRE(gen.num_elements() == col.null_count());
    REQUIRE_NOTHROW(col.set_null_count(unknown_null_count));
    REQUIRE(gen.num_elements() == col.null_count());
}

TEMPLATE_TEST_CASE("reset null count all valid", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    REQUIRE(0 == col.null_count());
    REQUIRE_NOTHROW(col.set_null_count(unknown_null_count));
    REQUIRE(0 == col.null_count());
}

TEMPLATE_TEST_CASE("copy data no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    REQUIRE(gen.type() == col.type());
    REQUIRE_FALSE(col.nullable());
    REQUIRE(0 == col.null_count());
    REQUIRE(gen.num_elements() == col.size());
    REQUIRE(0 == col.num_children());
    verify_column_views(col, &resource);
    column_view v = col;
    REQUIRE(v.head() != gen.data.data());
    REQUIRE(equal(v.head(), gen.data.data(), gen.data.size()));
}

TEMPLATE_TEST_CASE("move data no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    void* original_data = gen.data.data();
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    REQUIRE(gen.type() == col.type());
    REQUIRE_FALSE(col.nullable());
    REQUIRE(0 == col.null_count());
    REQUIRE(gen.num_elements() == col.size());
    REQUIRE(0 == col.num_children());
    verify_column_views(col, &resource);
    column_view v = col;
    REQUIRE(v.head() == original_data);
}

TEMPLATE_TEST_CASE("copy data and mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource,
                 gen.type(),
                 gen.num_elements(),
                 core::buffer{&resource, gen.data},
                 core::buffer{&resource, gen.all_valid_mask}};
    REQUIRE(gen.type() == col.type());
    REQUIRE(col.nullable());
    REQUIRE(0 == col.null_count());
    REQUIRE(gen.num_elements() == col.size());
    REQUIRE(0 == col.num_children());
    verify_column_views(col, &resource);
    column_view v = col;
    REQUIRE(v.head() != gen.data.data());
    REQUIRE(v.null_mask() != gen.all_valid_mask.data());
    REQUIRE(equal(v.head(), gen.data.data(), gen.data.size()));
    REQUIRE(equal(v.null_mask(), gen.all_valid_mask.data(), gen.all_valid_mask.size()));
}

TEMPLATE_TEST_CASE("move data and mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    void* original_data = gen.data.data();
    void* original_mask = gen.all_valid_mask.data();
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    REQUIRE(gen.type() == col.type());
    REQUIRE(col.nullable());
    REQUIRE(0 == col.null_count());
    REQUIRE(gen.num_elements() == col.size());
    REQUIRE(0 == col.num_children());
    verify_column_views(col, &resource);
    column_view v = col;
    REQUIRE(v.head() == original_data);
    REQUIRE(v.null_mask() == original_mask);
}

TEMPLATE_TEST_CASE("copy constructor no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t original{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    column_t copy{&resource, original};
    verify_column_views(copy, &resource);
    REQUIRE(equal(original, copy));
    column_view original_view = original;
    column_view copy_view = copy;
    REQUIRE(original_view.head() != copy_view.head());
}

TEMPLATE_TEST_CASE("copy constructor with mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t original{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    column_t copy{&resource, original};
    verify_column_views(copy, &resource);
    REQUIRE(equal(original, copy));
    column_view original_view = original;
    column_view copy_view = copy;
    REQUIRE(original_view.head() != copy_view.head());
    REQUIRE(original_view.null_mask() != copy_view.null_mask());
}

TEMPLATE_TEST_CASE("move constructor no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t original{&resource, gen.type(), gen.num_elements(), std::move(gen.data), core::buffer{&resource}};
    auto original_data = original.view().head();
    column_t moved_to{std::move(original)};
    REQUIRE(0 == original.size());
    REQUIRE(data_type{type_id::empty} == original.type());
    verify_column_views(moved_to, &resource);
    column_view moved_to_view = moved_to;
    REQUIRE(original_data == moved_to_view.head());
}

TEMPLATE_TEST_CASE("move constructor with mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t original{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    auto original_data = original.view().head();
    auto original_mask = original.view().null_mask();
    column_t moved_to{std::move(original)};
    verify_column_views(moved_to, &resource);
    REQUIRE(0 == original.size());
    REQUIRE(data_type{type_id::empty} == original.type());
    column_view moved_to_view = moved_to;
    REQUIRE(original_data == moved_to_view.head());
    REQUIRE(original_mask == moved_to_view.null_mask());
}

TEMPLATE_TEST_CASE("device uvector constructor no mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    core::uvector<TestType> original(&resource, static_cast<std::size_t>(gen.num_elements()));
    std::copy(static_cast<TestType*>(gen.data.data()),
              static_cast<TestType*>(gen.data.data()) + gen.num_elements(),
              original.begin());
    auto original_data = original.data();
    column_t moved_to{&resource, std::move(original), core::buffer{&resource}};
    verify_column_views(moved_to, &resource);
    column_view moved_to_view = moved_to;
    REQUIRE(original_data == moved_to_view.head());
}

TEMPLATE_TEST_CASE("device uvector constructor with mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    core::uvector<TestType> original(&resource, static_cast<std::size_t>(gen.num_elements()));
    std::copy(static_cast<TestType*>(gen.data.data()),
              static_cast<TestType*>(gen.data.data()) + gen.num_elements(),
              original.begin());
    auto original_data = original.data();
    auto original_mask = gen.all_valid_mask.data();
    column_t moved_to{&resource, std::move(original), std::move(gen.all_valid_mask)};
    verify_column_views(moved_to, &resource);
    column_view moved_to_view = moved_to;
    REQUIRE(original_data == moved_to_view.head());
    REQUIRE(original_mask == moved_to_view.null_mask());
}

TEMPLATE_TEST_CASE("construct with children", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    std::vector<std::unique_ptr<column_t>> children;
    children.emplace_back(std::make_unique<column_t>(&resource,
                                                     data_type{type_id::int8},
                                                     42,
                                                     core::buffer(&resource, gen.data),
                                                     core::buffer(&resource, gen.all_valid_mask)));
    children.emplace_back(std::make_unique<column_t>(&resource,
                                                     data_type{type_id::float64},
                                                     314,
                                                     core::buffer(&resource, gen.data),
                                                     core::buffer{&resource, gen.all_valid_mask}));
    column_t col{&resource,
                 gen.type(),
                 gen.num_elements(),
                 core::buffer(&resource, gen.data),
                 core::buffer{&resource, gen.all_valid_mask},
                 unknown_null_count,
                 std::move(children)};
    verify_column_views(col, &resource);
    REQUIRE(2 == col.num_children());
    REQUIRE(data_type{type_id::int8} == col.child(0).type());
    REQUIRE(42 == col.child(0).size());
    REQUIRE(data_type{type_id::float64} == col.child(1).type());
    REQUIRE(314 == col.child(1).size());
}

TEMPLATE_TEST_CASE("release no children", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t col{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    auto original_data = col.view().head();
    auto original_mask = col.view().null_mask();
    column_t::contents contents = col.release();
    REQUIRE(original_data == contents.data->data());
    REQUIRE(original_mask == contents.null_mask->data());
    REQUIRE(0u == contents.children.size());
    REQUIRE(0 == col.size());
    REQUIRE(0 == col.null_count());
    REQUIRE(data_type{type_id::empty} == col.type());
    REQUIRE(0 == col.num_children());
}

TEMPLATE_TEST_CASE("release with children", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    std::vector<std::unique_ptr<column_t>> children;

    children.emplace_back(std::make_unique<column_t>(&resource,
                                                     gen.type(),
                                                     gen.num_elements(),
                                                     core::buffer(&resource, gen.data),
                                                     core::buffer(&resource, gen.all_valid_mask)));
    children.emplace_back(std::make_unique<column_t>(&resource,
                                                     gen.type(),
                                                     gen.num_elements(),
                                                     core::buffer(&resource, gen.data),
                                                     core::buffer(&resource, gen.all_valid_mask)));

    column_t col{&resource,
                 gen.type(),
                 gen.num_elements(),
                 core::buffer(&resource, gen.data),
                 core::buffer(&resource, gen.all_valid_mask),
                 unknown_null_count,
                 std::move(children)};

    auto original_data = col.view().head();
    auto original_mask = col.view().null_mask();

    column_t::contents contents = col.release();
    REQUIRE(original_data == contents.data->data());
    REQUIRE(original_mask == contents.null_mask->data());
    REQUIRE(2u == contents.children.size());
    REQUIRE(0 == col.size());
    REQUIRE(0 == col.null_count());
    REQUIRE(data_type{type_id::empty} == col.type());
    REQUIRE(0 == col.num_children());
}

TEMPLATE_TEST_CASE("column view constructor with mask", "[column][template]", std::int32_t) {
    auto resource = std::pmr::synchronized_pool_resource();
    gen_column<TestType> gen(&resource);
    column_t original{&resource, gen.type(), gen.num_elements(), std::move(gen.data), std::move(gen.all_valid_mask)};
    column_view original_view = original;
    column_t copy{&resource, original_view};
    verify_column_views(copy, &resource);
    REQUIRE(equal(original, copy));
    column_view copy_view = copy;
    REQUIRE(original_view.head() != copy_view.head());
    REQUIRE(original_view.null_mask() != copy_view.null_mask());
}
