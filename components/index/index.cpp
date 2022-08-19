#include "index.hpp"

namespace components::index {

    index_t::index_t(std::pmr::memory_resource* resource, components::ql::index_type type, const keys_base_storage_t& keys)
        : resource_(resource)
        , type_(type)
        , keys_(keys) {
              assert(resource!= nullptr);
    }

    index_t::range index_t::find(const value_t& value) const {
        return find_impl(value);
    }

    index_t::range index_t::lower_bound(const query_t& query) const {
        return lower_bound_impl(query);
    }

    index_t::range index_t::upper_bound(const query_t& query) const {
        return upper_bound_impl(query);
    }

//    index_t::iterator index_t::cbegin() const {
//        return cbegin_impl();
//    }
//
//    index_t::iterator index_t::cend() const {
//        return cend_impl();
//    }

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
        impl_->next();
        return *this;
    }

//    bool index_t::iterator_t::operator==(const iterator_t& other) const {
//        return impl_->equals(other.impl_);
//    }
//
//    bool index_t::iterator_t::operator!=(const iterator_t& other) const {
//        return impl_->not_equals(other.impl_);
//    }

    index_t::iterator_t::iterator_t(index_t::iterator_t::iterator_impl_t* ptr)
        : impl_(ptr) {}

    index_t::iterator_t::~iterator_t() {
        delete impl_; //todo
    }

    index_t::~index_t() = default;

} // namespace components::index