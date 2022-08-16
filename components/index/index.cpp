#include "index.hpp"

namespace components::index {

    index_t::index_t(std::pmr::memory_resource* resource, components::ql::index_type type, const keys_base_storage_t& keys)
        : resource_(resource)
        , type_(type)
        , keys_(keys) {
              assert(resource!= nullptr);
    }

    index_t::iterator index_t::lower_bound(const query_t& values) const {
        return lower_bound_impl(values);
    }

    index_t::iterator index_t::upper_bound(const query_t& values) const {
        return upper_bound_impl(values);
    }

    index_t::iterator index_t::cbegin() const {
        return cbegin_impl();
    }

    index_t::iterator index_t::cend() const {
        return cend_impl();
    }

    auto index_t::insert(value_t key, doc_t value) -> void {
        return insert_impl(key, value);
    }

    auto index_t::keys() -> std::pair<std::pmr::vector<key_t>::iterator, std::pmr::vector<key_t>::iterator> {
        return std::make_pair(keys_.begin(), keys_.end());
    }

    std::pmr::memory_resource* index_t::resource() const noexcept {
        return resource_;
    }

    ql::index_type index_t::type() const noexcept {
        return type_;
    }

    const document_ptr& index_t::iterator_t::operator*() const {
        return impl_->value_ref();
    }

    index_t::iterator_t& index_t::iterator_t::operator++() {
        return impl_->next();
    }

    bool index_t::iterator_t::operator==(const iterator_t& other) const {
        return impl_->equals(other);
    }

    bool index_t::iterator_t::operator!=(const iterator_t& other) const {
        return impl_->not_equals(other);
    }

    index_t::iterator_t::~iterator_t() = default;

    index_t::~index_t() = default;

} // namespace components::index