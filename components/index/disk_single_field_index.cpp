#include "disk_single_field_index.hpp"

namespace components::index {

    disk_single_field_index_t::disk_single_field_index_t(std::pmr::memory_resource* resource, std::string name, const keys_base_storage_t& keys)
        : index_t(resource, ql::index_type::single, std::move(name), keys)
        , actor_zeta::basic_async_actor(manager, name) {}

    disk_single_field_index_t::~disk_single_field_index_t() = default;

    index_t::iterator::reference disk_single_field_index_t::impl_t::value_ref() const {
        return *iterator_;
    }

    index_t::iterator_t::iterator_impl_t* disk_single_field_index_t::impl_t::next() {
        iterator_++;
        return this;
    }

    bool disk_single_field_index_t::impl_t::equals(const iterator_impl_t* other) const {
        return iterator_ == dynamic_cast<const impl_t *>(other)->iterator_; //todo
    }

    bool disk_single_field_index_t::impl_t::not_equals(const iterator_impl_t* other) const {
        return iterator_ != dynamic_cast<const impl_t *>(other)->iterator_; //todo
    }

    index_t::iterator::iterator_impl_t *disk_single_field_index_t::impl_t::copy() const {
        return new impl_t(*this);
    }

    disk_single_field_index_t::impl_t::impl_t(const_iterator iterator)
        : iterator_(iterator) {
    }

    auto disk_single_field_index_t::insert_impl(value_t key, index_value_t value) -> void {
        //todo: impl
    }

    auto disk_single_field_index_t::remove_impl(components::index::value_t key) -> void {
        //todo: impl
    }

    index_t::range disk_single_field_index_t::find_impl(const value_t& value) const {
        //todo: impl
        return std::make_pair(cbegin(), cend());
    }

    index_t::range disk_single_field_index_t::lower_bound_impl(const value_t& value) const {
        //todo: impl
        return std::make_pair(cbegin(), cend());
    }

    index_t::range disk_single_field_index_t::upper_bound_impl(const value_t& value) const {
        //todo: impl
        return std::make_pair(cbegin(), cend());
    }

    index_t::iterator disk_single_field_index_t::cbegin_impl() const {
        return index_t::iterator(new impl_t(buffer_.cbegin()));
    }

    index_t::iterator disk_single_field_index_t::cend_impl() const {
        return index_t::iterator(new impl_t(buffer_.cend()));
    }

} // namespace components::index
