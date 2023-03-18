#include "index_engine.hpp"

#include <utility>
#include <iostream>

#include "components/document/mutable/mutable_dict.hpp"
#include "core/pmr.hpp"

namespace components::index {

    void find(const index_engine_ptr&, query_t,result_set_t*) {
        /// auto* index  = search_index(ptr, query);
        /// index->find(std::move(query),set);
    }

    void find(const index_engine_ptr&, id_index, result_set_t*) {
        /// auto* index  = search_index(ptr, id);
        /// index->find(id,set);
    }

    void insert(const index_engine_ptr& ptr, id_index id, std::pmr::vector<document_ptr>& docs) {
        auto* index = search_index(ptr, id);
        for (const auto& i : docs) {
            auto range = index->keys();
            for (auto j = range.first; j != range.second; ++j) {
                const auto& key_tmp = *j;
                const std::string& key = key_tmp.as_string(); // hack
                document::document_view_t view(i);
                if ((!(view.is_null(key)))) {
                    auto* data = view.get_value(key);
                    ::document::wrapper_value_t key_(data);
                    index->insert(key_, i);
                }
            }
        }
    }

    void insert(const index_engine_ptr& ptr, id_index id, core::pmr::btree::btree_t<document::document_id_t, document_ptr> &docs) {
        auto* index = search_index(ptr, id);
        for (auto &doc : docs) {
            auto range = index->keys();
            for (auto j = range.first; j != range.second; ++j) {
                const auto& key_tmp = *j;
                const std::string& key = key_tmp.as_string(); // hack
                document::document_view_t view(doc.second);
                if ((!(view.is_null(key)))) {
                    auto* data = view.get_value(key);
                    ::document::wrapper_value_t key_(data);
                    index->insert(key_, {doc.first, doc.second});
                }
            }
        }
    }

    void insert_one(const index_engine_ptr& ptr, id_index id, document_ptr doc) {
        auto* index = search_index(ptr, id);
        auto range = index->keys();
        for (auto j = range.first; j != range.second; ++j) {
            if (j->which() == key_t::type::string) {
                const auto& key_tmp = *j;
                const std::string& key = key_tmp.as_string(); // hack
                document::document_view_t view(doc);
                if ((!(view.is_null(key)))) {
                    auto* data = view.get_value(key);
                    ::document::wrapper_value_t key_(data);
                    index->insert(key_ , doc);
                }
            }
        }
    }

    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t::pointer {
        return ptr->matching(id);
    }

    auto search_index(const index_engine_ptr& ptr, const keys_base_storage_t & query) -> index_t::pointer {
        return ptr->matching(query);
    }

    auto make_index_engine(actor_zeta::detail::pmr::memory_resource* resource) -> index_engine_ptr {
        auto size = sizeof(index_engine_t);
        auto align = alignof(index_engine_t);
        auto* buffer = resource->allocate(size, align);
        auto* index_engine = new (buffer) index_engine_t(resource);
        return {index_engine, core::pmr::deleter_t(resource)};
    }

    bool is_match_document(const index_ptr& index, const document_ptr& document) {
        auto keys = index->keys();
        components::document::document_view_t view(document);
        for (auto key = keys.first; key != keys.second; ++key) {
            if (!view.is_exists(key->as_string())) {
                return false;
            }
        }
        return true;
    }

    components::index::value_t get_value_by_index(const index_ptr& index, const document_ptr& document) {
        auto keys = index->keys();
        components::document::document_view_t view(document);
        if (keys.first != keys.second) {
            return components::index::value_t(view.get_value(keys.first->as_string()));
            //todo: multi values index
        }
        return components::index::value_t(nullptr);
    }


    index_engine_t::index_engine_t(actor_zeta::detail::pmr::memory_resource* resource)
        : resource_(resource)
        , mapper_(resource)
        , index_to_mapper_(resource)
        , storage_(resource){
    }

    auto index_engine_t::add_index(const keys_base_storage_t & keys, index_ptr index) -> uint32_t {
        auto end = storage_.cend();
        auto d = storage_.insert(end, std::move(index));
        mapper_.emplace(keys, d);
        auto new_id = index_to_mapper_.size();
        index_to_mapper_.emplace(new_id, d);
        return uint32_t(new_id);
    }

    actor_zeta::detail::pmr::memory_resource* index_engine_t::resource() noexcept {
        return resource_;
    }

    auto index_engine_t::matching(id_index id) -> index_t::pointer {
        return index_to_mapper_.find(id)->second->get();
    }

    auto index_engine_t::size() const -> std::size_t {
        return mapper_.size();
    }

    auto index_engine_t::matching(const keys_base_storage_t& query) -> index_t::pointer {
        auto it = mapper_.find(query);
        if (it != mapper_.end()) {
            return it->second->get();
        }
        return nullptr;
    }

    void index_engine_t::insert_document(const document_ptr& document) {
        for (auto &index : storage_) {
            if (is_match_document(index, document)) {
                index->insert(get_value_by_index(index, document), document);
            }
        }
    }

    void index_engine_t::delete_document(const document_ptr& document) {
        for (auto &index : storage_) {
            if (is_match_document(index, document)) {
                index->remove(get_value_by_index(index, document));
            }
        }
    }

    void set_disk_agent(const index_engine_ptr& ptr, id_index id, actor_zeta::address_t address) {
        auto* index = search_index(ptr, id);
        if (index) {
            index->set_disk_agent(std::move(address));
        }
    }

} // namespace components::index
