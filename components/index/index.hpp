#pragma once
#include <map>
#include <memory>
#include <scoped_allocator>
#include <utility>

#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/log/log.hpp>
#include <components/parser/conditional_expression.hpp>
#include <components/session/session.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/polymorphic_allocator.hpp>

namespace components::index {

    using document_ptr = components::document::document_ptr;

    class query_t {

    };

    class result_set_t {

    };

    class index_t {
    public:
        virtual ~index_t();
        auto insert();
        auto find(query_t query) -> result_set_t;

    protected:
        explicit index_t(actor_zeta::detail::pmr::memory_resource* resource);
        virtual auto insert_impl() -> void = 0;
        virtual auto find_impl(query_t) -> result_set_t = 0;
        actor_zeta::detail::pmr::memory_resource* resource_;
    };

    using index_raw_ptr = index_t*;
    using index_ptr = std::unique_ptr<index_t>;
    using keys_base_t = std::vector<std::string>;

    struct index_engine_t final {
    public:
        using value_t = index_ptr;

        explicit index_engine_t(actor_zeta::detail::pmr::memory_resource* resource);
        auto find(keys_base_t keys) -> index_raw_ptr;
        auto emplace(keys_base_t keys, value_t value) -> void;
        [[nodiscard]] auto size() const -> std::size_t;
        actor_zeta::detail::pmr::memory_resource* resource() noexcept;

    private:
        /*
    struct keys_t {
        using key_t = std::string;
        using comparator_t = std::less<keys_t>;
        using allocator_t = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<keys_t>>;
        bool operator<(const keys_t& keys) const {
            return keys_ < keys.keys_;
        }
        bool operator<(const keys_base_t & keys) const {
            return keys_ < keys;
        }
        const std::vector<std::string> keys_;
        const std::string name_;
        index_ptr index_;
    };*/

        using comparator_t = std::less<keys_base_t>;
        using allocator_t = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<std::pair<const keys_base_t, value_t>>>;
        /// using storage_t = std::map<key_t, value_t, comparator_t, allocator_t>;
        using mapper_t = std::map<keys_base_t, index_ptr, comparator_t, allocator_t>;
        actor_zeta::detail::pmr::memory_resource* resource_;
        mapper_t mapper_;
    };

    class deleter final {
    public:
        explicit deleter(actor_zeta::detail::pmr::memory_resource* ptr);

        template<class T>
        void operator()(T* target) {
            target->~T();
            ptr_->deallocate(target, sizeof(T));
        }

    private:
        actor_zeta::detail::pmr::memory_resource* ptr_;
    };

    using index_engine_ptr = std::unique_ptr<index_engine_t, deleter>;

    auto make_index_engine(actor_zeta::detail::pmr::memory_resource* resource) -> index_engine_ptr;
    auto search_index(const index_engine_ptr& ptr, keys_base_t keys) -> index_t*;

    template<class Target, class... Args>
    auto make_index(index_engine_ptr& ptr, keys_base_t&& keys, Args&&... args) {
        ptr->emplace(std::move(keys), std::make_unique<Target>(ptr->resource(), std::forward<Args>(args)...));
    }

    void insert(const index_engine_ptr& index, keys_base_t params, std::vector<document_ptr>);
    void find(const index_engine_ptr& index, keys_base_t params, query_t query);

} // namespace components::index