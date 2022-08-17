#include "single_field_index.hpp"

namespace components::index {

    single_field_index_t::single_field_index_t(std::pmr::memory_resource* resource, const keys_base_storage_t& keys)
        : index_t(resource, ql::index_type::single, keys)
        , storage_(resource) {}

    single_field_index_t::~single_field_index_t() = default;

    index_t::iterator::reference single_field_index_t::impl_t::value_ref() const {
        return iterator_->second;
    }
    index_t::iterator_t::iterator_impl_t* single_field_index_t::impl_t::next() {
        iterator_++;
        return this;
    }
    bool single_field_index_t::impl_t::equals(const iterator_t& other) const {}
    bool single_field_index_t::impl_t::not_equals(const iterator_t& other) const {}
    single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_(iterator) {
    }

    auto single_field_index_t::insert_impl(value_t key, components::index::document_ptr value) -> void {
        storage_.emplace(key, value);
    }

    index_t::iterator single_field_index_t::lower_bound_impl(const query_t& values) const {
        auto it = storage_.lower_bound()
    }
    index_t::iterator single_field_index_t::upper_bound_impl(const query_t& values) const {}
    index_t::iterator single_field_index_t::cbegin_impl() const {}
    index_t::iterator single_field_index_t::cend_impl() const {}

} // namespace components::index
