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

    bool single_field_index_t::impl_t::equals(const iterator_impl_t* other) const {
        return iterator_ == dynamic_cast<const impl_t *>(other)->iterator_; //todo
    }

    bool single_field_index_t::impl_t::not_equals(const iterator_impl_t* other) const {
        return iterator_ != dynamic_cast<const impl_t *>(other)->iterator_; //todo
    }

    single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_(iterator) {
    }

    auto single_field_index_t::insert_impl(value_t key, components::index::document_ptr value) -> void {
        storage_.emplace(key, value);
    }

    index_t::range single_field_index_t::find_impl(const value_t& value) const {
        auto it = storage_.find(value);
        if (it != storage_.cend()) {
            auto first = iterator(new impl_t(it));
            auto second = iterator(new impl_t(++it));
            return std::make_pair(first, second);
        }
        return std::make_pair(cend(), cend());
    }

    index_t::range single_field_index_t::lower_bound_impl(const value_t& value) const {
        auto it = storage_.lower_bound(value);
        return std::make_pair(cbegin(), index_t::iterator(new impl_t(it)));
    }

    index_t::range single_field_index_t::upper_bound_impl(const value_t& value) const {
        auto it = storage_.upper_bound(value);
        return std::make_pair(index_t::iterator(new impl_t(it)), cend());
    }

    index_t::iterator single_field_index_t::cbegin_impl() const {
        return index_t::iterator(new impl_t(storage_.cbegin()));
    }

    index_t::iterator single_field_index_t::cend_impl() const {
        return index_t::iterator(new impl_t(storage_.cend()));
    }

} // namespace components::index
