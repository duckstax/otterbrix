#include <catch2/catch.hpp>

#include "components/index/index_engine.hpp"
#include "components/index/single_field_index.hpp"
#include "components/tests/generaty.hpp"

using namespace components::index;
using key = components::expressions::key_t;

// TODO move to test utils

// template <class memory_pool_t>
// class MemoryTracerResource : public memory_pool_t {
// public:
//     MemoryTracerResource()
//         : memory_pool_t( std::pmr::get_default_resource()) {}
//     virtual ~MemoryTracerResource() = default;

//     void release() { memory_pool_t::release(); }

//     std::pmr::memory_resource* upstream_resource() const noexcept {
//         return memory_pool_t::upstream_resource();
//     }

// protected:
//     void* do_allocate(size_t bytes, size_t alignment) override {
//         void * ptr = memory_pool_t::do_allocate(bytes, alignment);
//         std::cout << "do_allocate: bytes: " << bytes << std::endl;
//         std::cout << "do_allocate: alignment: " << alignment << std::endl;
//         std::cout << "do_allocate: region: " << ptr << " - " << ptr + bytes << std::endl;
//         return ptr;
//     }

//     void do_deallocate(void* p, size_t bytes, size_t alignment) override {
//         std::cout << "do_deallocate: region: " << p << " - " << p + bytes << std::endl;
//         std::cout << "do_deallocate: bytes: " << bytes << std::endl;
//         std::cout << "do_deallocate: alignment: " << alignment << std::endl;
//         memory_pool_t::do_deallocate(p, bytes, alignment);
//     }

//     bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
// };

TEST_CASE("single_field_index:base") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto tape = std::make_unique<impl::base_document>(&resource);
    single_field_index_t index(&resource, "single_count", {key("count")});
    for (int i : {0, 1, 10, 5, 6, 2, 8, 13}) {
        auto doc = gen_doc(i, &resource);
        auto value = doc->get_value(std::string_view("count"));
        index.insert(value, std::move(doc));
    }
    {
        value_t value(tape.get(), 10);
        auto find_range = index.find(value);
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(find_range.first->doc->get_long("count") == 10);
        REQUIRE(find_range.first->doc->get_string("countStr") == "10");
        REQUIRE(++find_range.first == find_range.second);
    }
    {
        value_t value(tape.get(), 11);
        auto find_range = index.find(value);
        REQUIRE(find_range.first == find_range.second);
    }
    {
        value_t value(tape.get(), 4);
        auto find_range = index.lower_bound(value);
        REQUIRE(find_range.first == index.cbegin());
        REQUIRE(find_range.first->doc->get_long("count") == 0);
        REQUIRE((++find_range.first)->doc->get_long("count") == 1);
        REQUIRE((++find_range.first)->doc->get_long("count") == 2);
        REQUIRE(++find_range.first == find_range.second);
    }
    {
        value_t value(tape.get(), 5);
        auto find_range = index.lower_bound(value);
        REQUIRE(find_range.first == index.cbegin());
        REQUIRE(find_range.first->doc->get_long("count") == 0);
        REQUIRE((++find_range.first)->doc->get_long("count") == 1);
        REQUIRE((++find_range.first)->doc->get_long("count") == 2);
        REQUIRE(++find_range.first == find_range.second);
    }
    {
        value_t value(tape.get(), 6);
        auto find_range = index.upper_bound(value);
        REQUIRE(find_range.second == index.cend());
        REQUIRE(find_range.first->doc->get_long("count") == 8);
        REQUIRE((++find_range.first)->doc->get_long("count") == 10);
        REQUIRE((++find_range.first)->doc->get_long("count") == 13);
        REQUIRE(++find_range.first == find_range.second);
    }
    {
        value_t value(tape.get(), 7);
        auto find_range = index.upper_bound(value);
        REQUIRE(find_range.second == index.cend());
        REQUIRE(find_range.first->doc->get_long("count") == 8);
        REQUIRE((++find_range.first)->doc->get_long("count") == 10);
        REQUIRE((++find_range.first)->doc->get_long("count") == 13);
        REQUIRE(++find_range.first == find_range.second);
    }
    {
        for (int i : {0, 1, 10, 5, 6, 2, 8, 13}) {
            auto doc = gen_doc(i, &resource);
            index.insert(doc->get_value(std::string_view("count")), doc);
        }
        value_t value(tape.get(), 10);
        auto find_range = index.find(value);
        REQUIRE(find_range.first != find_range.second);
        REQUIRE(std::distance(find_range.first, find_range.second) == 2);
        REQUIRE(find_range.first->doc->get_long("count") == 10);
        REQUIRE(find_range.first->doc->get_string("countStr") == "10");
        ++find_range.first;
        REQUIRE(find_range.first->doc->get_long("count") == 10);
        REQUIRE(find_range.first->doc->get_string("countStr") == "10");
        REQUIRE(++find_range.first == find_range.second);
    }
}

TEST_CASE("single_field_index:engine") {
    auto resource = std::pmr::synchronized_pool_resource();
    auto index_engine = make_index_engine(&resource);
    auto id = make_index<single_field_index_t>(index_engine, "single_count", {key("count")});
    insert_one(index_engine, id, gen_doc(0, &resource));
    std::pmr::vector<document_ptr> data;
    for (int i = 10; i >= 1; --i) {
        data.push_back(gen_doc(i, &resource));
    }

    insert(index_engine, id, data);
    auto address = actor_zeta::address_t::empty_address();
    ///result_set_t set(resource, address);

    std::string value = R"({"count": {"$gt": 10}})";
    auto d = components::document::document_t::document_from_json(value, &resource);
    //auto condition = components::ql::parse_find_condition(d);
    ///condition->type_
    /// find(index_engine, query, &set);
}