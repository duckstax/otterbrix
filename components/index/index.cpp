#include "index.hpp"

namespace components::index {

    index_t::index_t(std::pmr::memory_resource* resource,
                     components::logical_plan::index_type type,
                     std::string name,
                     const keys_base_storage_t& keys)
        : resource_(resource)
        , type_(type)
        , name_(std::move(name))
        , keys_(keys) {
        assert(resource != nullptr);
    }

    document::impl::base_document* index_t::tape() { return tape_.get(); }

    index_t::range index_t::find(const value_t& value) const { return find_impl(value); }

    index_t::range index_t::lower_bound(const value_t& value) const { return lower_bound_impl(value); }

    index_t::range index_t::upper_bound(const value_t& value) const { return upper_bound_impl(value); }

    index_t::iterator index_t::cbegin() const { return cbegin_impl(); }

    index_t::iterator index_t::cend() const { return cend_impl(); }

    auto index_t::insert(value_t key, index_value_t value) -> void { return insert_impl(key, std::move(value)); }

    auto index_t::insert(value_t key, const document::document_id_t& id) -> void {
        return insert_impl(key, {id, nullptr});
    }

    auto index_t::insert(value_t key, document::document_ptr doc) -> void {
        auto id = document::get_document_id(doc);
        return insert_impl(key, {id, std::move(doc)});
    }

    auto index_t::insert(document::document_ptr doc) -> void { insert_impl(std::move(doc)); }

    auto index_t::remove(value_t key) -> void { remove_impl(key); }

    auto index_t::keys() -> std::pair<std::pmr::vector<key_t>::iterator, std::pmr::vector<key_t>::iterator> {
        return std::make_pair(keys_.begin(), keys_.end());
    }

    std::pmr::memory_resource* index_t::resource() const noexcept { return resource_; }

    logical_plan::index_type index_t::type() const noexcept { return type_; }

    const std::string& index_t::name() const noexcept { return name_; }

    bool index_t::is_disk() const noexcept { return disk_agent_ != actor_zeta::address_t::empty_address(); }

    const actor_zeta::address_t& index_t::disk_agent() const noexcept { return disk_agent_; }

    void index_t::set_disk_agent(actor_zeta::address_t address) noexcept { disk_agent_ = std::move(address); }

    void index_t::clean_memory_to_new_elements(std::size_t count) noexcept { clean_memory_to_new_elements_impl(count); }

    index_t::iterator_t::reference index_t::iterator_t::operator*() const { return impl_->value_ref(); }

    index_t::iterator_t::pointer index_t::iterator_t::operator->() const { return &impl_->value_ref(); }

    index_t::iterator_t& index_t::iterator_t::operator++() {
        impl_->next();
        return *this;
    }

    bool index_t::iterator_t::operator==(const iterator_t& other) const { return impl_->equals(other.impl_); }

    bool index_t::iterator_t::operator!=(const iterator_t& other) const { return impl_->not_equals(other.impl_); }

    index_t::iterator_t::iterator_t(index_t::iterator_t::iterator_impl_t* ptr)
        : impl_(ptr) {}

    index_t::iterator_t::~iterator_t() { delete impl_; }

    index_t::iterator_t::iterator_t(const iterator_t& other)
        : impl_(other.impl_->copy()) {}

    index_t::iterator_t& index_t::iterator_t::operator=(const iterator_t& other) {
        delete impl_;
        impl_ = other.impl_->copy();
        return *this;
    }

    index_t::~index_t() = default;

} // namespace components::index