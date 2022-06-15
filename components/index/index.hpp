#pragma once
#include <map>
#include <memory>
#include <scoped_allocator>

#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/log/log.hpp>
#include <components/parser/conditional_expression.hpp>
#include <components/session/session.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/polymorphic_allocator.hpp>

class result_set {

};

class index_t {
public:
    index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : resource_(resource) {}

    auto insert() {
        return insert_impl();
    }

    auto find() {
        return find_impl();
    }

protected:
    virtual auto insert_impl() -> void = 0;
    virtual auto find_impl() -> void = 0;

private:
    actor_zeta::detail::pmr::memory_resource* resource_;
};

using index_ptr = std::unique_ptr<index_t>;

using index_engine_t = std::map<int, index_ptr>;

using index_engine_ptr = std::unique_ptr<index_engine_t>;

auto search_index(const index_engine_ptr& ptr, int params) -> index_t* {
    return ptr->find(params)->second.get();
}

template<class Target, class Key, class... Args>
auto make_index(index_engine_ptr& ptr, Key key, Args&&... args) {
    ptr->emplace(std::forward<Key>(key), std::make_unique<Target>(std::forward<Args>(args)...));
}

void insert(const index_engine_ptr& index, int params) {
    search_index(index, params)->insert();
}

void find(const index_engine_ptr& index, int params) {
    search_index(index, params)->find();
}



class single_field_index_t final : public index_t {
public:
    using key_t = document::impl::value_t;
    using value_t = components::document::document_ptr;
    using comparator_t = std::less<key_t>;
    using allocator_t = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
    using storage_t = std::map<key_t, value_t, comparator_t, allocator_t>;

    single_field_index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : index_t(resource)
        , data_(resource) {}

    auto insert_impl() {
    }

    auto find_impl() {
    }

private:
    storage_t data_;
};

/*
class composite_field_index_t final : public index_t {
public:
    using key_t = document::impl::value_t;
    using value_t = components::document::document_ptr;
    using comparator = std::less<key_t>;
    using allocator_type = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
    using storage = std::map<key_t, value_t, comparator, allocator_type>;

    composite_field_index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : data_(resource) {}

private:
    storage data_;
};

class multikey_index_t final : public index_t {
public:
    using key_t = document::impl::value_t;
    using value_t = components::document::document_ptr;
    using comparator = std::less<key_t>;
    using allocator_type = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
    using storage = std::multimap<key_t, value_t, comparator, allocator_type>;

    multikey_index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : data_(resource) {}

private:
    storage data_;
};

class hash_index_t final : public index_t {
public:
    using key_t = document::impl::value_t;
    using value_t = components::document::document_ptr;
    using comparator = std::less<key_t>;
    using allocator_type = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
    using storage = std::map<key_t, value_t, comparator, allocator_type>;

    hash_index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : data_(resource) {}

private:
    storage data_;
};

class wildcard_index_t final : public index_t {
public:
    using key_t = document::impl::value_t;
    using value_t = components::document::document_ptr;
    using comparator = std::less<key_t>;
    using allocator_type = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const key_t, value_t>>>;
    using storage = std::map<key_t, value_t, comparator, allocator_type>;

    wildcard_index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : data_(resource) {}

private:
    storage data_;
};
 */