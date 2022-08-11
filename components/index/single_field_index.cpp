#include "single_field_index.hpp"

namespace components::index {

    single_field_index_t::single_field_index_t(actor_zeta::detail::pmr::memory_resource* resource, const keys_base_t& keys)
        : index_t(resource, keys)
        , data_(resource) {}

    single_field_index_t::~single_field_index_t() = default;

    auto single_field_index_t::insert_impl(value_t key, components::index::document_ptr value) -> void {
        data_.emplace(key, value);
    }

    auto single_field_index_t::find_impl(query_t query, result_set_t* set) -> void {
        /* for (auto& i : query) {
             auto it = data_.find(i);
             set->append(it->second);
         }
         */
    }

} // namespace components::index
