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
#include <components/pipeline/context.hpp>

namespace components::index {

    struct index_engine_t final {
    public:
        explicit index_engine_t(actor_zeta::detail::pmr::memory_resource* resource);
        auto matching(id_index id) -> index_t::pointer;
        auto matching(const keys_base_storage_t& query) -> index_t::pointer;
        auto matching(const actor_zeta::address_t& address) -> index_t::pointer;
        auto add_index(const keys_base_storage_t&, index_ptr) -> uint32_t;
        auto add_disk_agent(id_index id, actor_zeta::address_t address) -> void;
        auto size() const -> std::size_t;
        actor_zeta::detail::pmr::memory_resource* resource() noexcept;

        void insert_document(const document_ptr& document, pipeline::context_t *pipeline_context);
        void delete_document(const document_ptr& document, pipeline::context_t *pipeline_context);

    private:
        using comparator_t = std::less<keys_base_storage_t>;
        using base_storage = std::pmr::list<index_ptr>;

        using keys_to_doc_t = std::pmr::map<keys_base_storage_t, base_storage::iterator, comparator_t>;
        using index_to_doc_t = std::pmr::unordered_map<id_index, base_storage::iterator>;
        using index_to_address_t = std::pmr::map<actor_zeta::address_t, base_storage::iterator>;

        actor_zeta::detail::pmr::memory_resource* resource_;
        keys_to_doc_t mapper_;
        index_to_doc_t index_to_mapper_;
        index_to_address_t index_to_address_;
        base_storage storage_;
    };

    using index_engine_ptr = core::pmr::unique_ptr<index_engine_t>;

    auto make_index_engine(actor_zeta::detail::pmr::memory_resource* resource) -> index_engine_ptr;
    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const keys_base_storage_t& query) -> index_t::pointer;
    auto search_index(const index_engine_ptr& ptr, const actor_zeta::address_t& address) -> index_t::pointer;

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

    void set_disk_agent(const index_engine_ptr& ptr, id_index id, const actor_zeta::address_t& address);
    void sync_index_from_disk(const index_engine_ptr& ptr,
                              const actor_zeta::address_t& index_address,
                              const std::pmr::vector<document::document_id_t>& ids,
                              const core::pmr::btree::btree_t<document::document_id_t, document_ptr> &storage);

} // namespace components::index