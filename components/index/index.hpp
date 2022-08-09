#pragma once

#include <map>
#include <memory>
#include <scoped_allocator>
#include <string>
#include <utility>
#include <vector>

#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/document/wrapper_value.hpp>
#include <components/log/log.hpp>
#include <components/parser/conditional_expression.hpp>
#include <components/ql/expr.hpp>
#include <components/session/session.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/polymorphic_allocator.hpp>

namespace components::index {

    using document_ptr = components::document::document_ptr;

    class result_set_t final {
    public:
        result_set_t(actor_zeta::detail::pmr::memory_resource* resource)
            : data_(resource) {}
        void append(document_ptr doc) {
            data_.emplace_back(doc);
        }

    private:
        std::pmr::vector<document_ptr> data_;
    };

    using key_t = ql::key_t;
    using keys_base_t = std::pmr::vector<key_t>;
    using id_index = uint32_t;
    using value_t = ::document::wrapper_value_t;
    using query_t = ql::expr_t::ptr;

    class index_t {
    public:
        using keys_storage = std::pmr::vector<key_t>;
        using doc_t = components::document::document_ptr;
        virtual ~index_t();
        void insert( value_t , doc_t);

        void find(query_t query, result_set_t*);
        void find(id_index, result_set_t*);

        [[nodiscard]] auto keys() -> std::pair<keys_storage::iterator, keys_storage::iterator>;

    protected:
        explicit index_t(actor_zeta::detail::pmr::memory_resource* resource, const keys_base_t& keys);
        virtual void insert_impl(value_t value_key, doc_t) = 0;
        virtual void find_impl(query_t, result_set_t*) = 0;
        actor_zeta::detail::pmr::memory_resource* resource_;
        keys_storage keys_;
    };

    using index_raw_ptr = index_t*;
    using index_ptr = std::unique_ptr<index_t>;

    struct index_engine_t final {
    public:
        using value_t = index_ptr;

        explicit index_engine_t(actor_zeta::detail::pmr::memory_resource* resource);
        auto find(id_index id) -> index_raw_ptr;
        auto find(const query_t& query) -> index_raw_ptr;
        auto emplace(const keys_base_t&, value_t) -> uint32_t;
        [[nodiscard]] auto size() const -> std::size_t;
        actor_zeta::detail::pmr::memory_resource* resource() noexcept;

    private:
        using comparator_t = std::less<keys_base_t>;
        using base_storgae = std::pmr::list<index_ptr>;

        using keys_to_doc_t = std::pmr::map<keys_base_t, base_storgae::iterator, comparator_t>;
        using index_to_doc_t = std::pmr::unordered_map<id_index, base_storgae::iterator>;

        actor_zeta::detail::pmr::memory_resource* resource_;
        keys_to_doc_t mapper_;
        index_to_doc_t index_to_mapper_;
        base_storgae storage_;
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
    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t*;
    auto search_index(const index_engine_ptr& ptr, const query_t& query) -> index_t*;

    template<class Target, class... Args>
    auto make_index(index_engine_ptr& ptr, const keys_base_t& keys, Args&&... args) -> uint32_t {
        return ptr->emplace(keys, std::make_unique<Target>(ptr->resource(), keys, std::forward<Args>(args)...));
    }

    void insert(const index_engine_ptr& ptr, id_index id, std::pmr::vector<document_ptr>& docs);
    void insert_one(const index_engine_ptr& ptr, id_index id, document_ptr docs);
    /// void find(const index_engine_ptr& index, id_index id , result_set_t* );
    /// void find(const index_engine_ptr& index, query_t query,result_set_t* );

} // namespace components::index