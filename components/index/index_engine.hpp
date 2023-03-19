#pragma once

#include <map>
#include <memory>
#include <scoped_allocator>
#include <string>
#include <utility>
#include <vector>

#include <core/btree/btree.hpp>
#include "core/pmr.hpp"
#include "forward.hpp"
#include "index.hpp"

namespace services::collection {
     class context_collection_t;

     namespace planner {
         class transaction_context_t;
     }
}

namespace components::index {

    struct index_engine_t final {
    public:
        explicit index_engine_t(actor_zeta::detail::pmr::memory_resource* resource);
        auto matching(id_index id) -> index_t::pointer;
        auto matching(const keys_base_storage_t& query) -> index_t::pointer;
        auto add_index(const keys_base_storage_t&, index_ptr) -> uint32_t;
        auto size() const -> std::size_t;
        actor_zeta::detail::pmr::memory_resource* resource() noexcept;

        void insert_document(const document_ptr& document,
                             services::collection::context_collection_t* context,
                             services::collection::planner::transaction_context_t *transaction_context);
        void delete_document(const document_ptr& document,
                             services::collection::context_collection_t* context,
                             services::collection::planner::transaction_context_t *transaction_context);

    private:
        using comparator_t = std::less<keys_base_storage_t>;
        using base_storgae = std::pmr::list<index_ptr>;

        using keys_to_doc_t = std::pmr::map<keys_base_storage_t, base_storgae::iterator, comparator_t>;
        using index_to_doc_t = std::pmr::unordered_map<id_index, base_storgae::iterator>;

        actor_zeta::detail::pmr::memory_resource* resource_;
        keys_to_doc_t mapper_;
        index_to_doc_t index_to_mapper_;
        base_storgae storage_;
    };

    using index_engine_ptr = core::pmr::unique_ptr<index_engine_t>;

    auto make_index_engine(actor_zeta::detail::pmr::memory_resource* resource) -> index_engine_ptr;
    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t*;
    auto search_index(const index_engine_ptr& ptr, const keys_base_storage_t& query) -> index_t*;

    template<class Target, class... Args>
    auto make_index(index_engine_ptr& ptr, std::string name, const keys_base_storage_t& keys, Args&&... args) -> uint32_t {
        return ptr->add_index(
            keys,
            core::pmr::make_unique<Target>(
                ptr->resource(),
                std::move(name),
                keys,
                std::forward<Args>(args).../*,
                core::pmr::deleter_t(ptr->resource())*/));
    }

    void insert(const index_engine_ptr& ptr, id_index id, std::pmr::vector<document_ptr>& docs);
    void insert(const index_engine_ptr& ptr, id_index id, core::pmr::btree::btree_t<document::document_id_t, document_ptr> &docs);
    void insert_one(const index_engine_ptr& ptr, id_index id, document_ptr docs);
    void find(const index_engine_ptr& index, id_index id, result_set_t*);
    void find(const index_engine_ptr& index, query_t query, result_set_t*);

    void set_disk_agent(const index_engine_ptr& ptr, id_index id, actor_zeta::address_t address);

} // namespace components::index