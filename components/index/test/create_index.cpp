#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/default_resource.hpp>
#include <actor-zeta/detail/pmr/memory_resource.hpp>

#include "components/index/index_engine.hpp"

using namespace components::index;

class dummy final : public index_t {
public:
    explicit dummy(actor_zeta::pmr::memory_resource* resource, const keys_base_storage_t& keys)
        : index_t(resource, components::ql::index_type::single, keys) {
    }

private:
    void insert_impl(value_t key, document_ptr value) {}
    iterator lower_bound_impl(const query_t& values) const {}
    iterator upper_bound_impl(const query_t& values) const {}
    iterator cbegin_impl() const {}
    iterator cend_impl() const {}

    class impl_t final : public index_t::iterator::iterator_impl_t {
    public:
        index_t::iterator::reference value_ref() const override {}
        iterator_t& next() override {}
        bool equals(const iterator_t& other) const {}
        bool not_equals(const iterator_t& other) const {}
    };
};

TEST_CASE("base index created") {
    actor_zeta::pmr::memory_resource* resource = actor_zeta::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    auto one_id = make_index<dummy>(index_engine, {"1"});
    auto two_id = make_index<dummy>(index_engine, {"1", "2"});
    auto two_1_id = make_index<dummy>(index_engine, {"2", "1"});
    REQUIRE(index_engine->size() == 3);
    REQUIRE(search_index(index_engine, one_id) != nullptr);
    REQUIRE(search_index(index_engine, two_id) != nullptr);
    REQUIRE(search_index(index_engine, two_1_id) != nullptr);
}