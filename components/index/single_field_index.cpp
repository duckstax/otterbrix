#include "single_field_index.hpp"

namespace components::index {

    single_field_index_t::single_field_index_t(std::pmr::memory_resource* resource, const keys_base_storage_t& keys)
        : index_t(resource,ql::index_type::single, keys)
        , data_(resource) {}

    single_field_index_t::~single_field_index_t() = default;

    auto single_field_index_t::insert_impl(value_t key, components::index::document_ptr value) -> void {
        data_.emplace(key, value);
    }

    index_t::iterator single_field_index_t::lower_bound_impl(const query_t& values) const  {}
    index_t::iterator single_field_index_t::upper_bound_impl(const query_t& values) const  {}
    index_t::iterator single_field_index_t::cbegin_impl() const  {}
    index_t::iterator single_field_index_t::cend_impl() const  {}

} // namespace components::index
