#include "index.hpp"

#include <utility>

#include "document/mutable/mutable_dict.hpp"

namespace components::index {

    void find(const index_engine_ptr& ptr, id_index id , query_t query,result_set_t* set) {
        auto* index  = search_index(ptr, id);
        index->find(std::move(query),set);
    }

    void insert(const index_engine_ptr& ptr, id_index id , std::vector<document_ptr>& docs) {
        auto* index = search_index(ptr, id);
        for (const auto& i : docs) {
            auto range =  index->keys();
            for (auto j = range.first;j!=range.second;++j) {
                const auto& key_tmp =*j;
                const std::string key(key_tmp); // hack
               document::document_view_t view(i);
                auto condishane = view.is_null(key) && (view.is_string(key) || view.is_long(key) || view.is_ulong(key));
                if(condishane){
                    index->insert(key_tmp,i);
                }
            }
        }
    }

    void insert_one(const index_engine_ptr& ptr, id_index id, document_ptr doc) {
        auto* index = search_index(ptr, id);
        auto range =  index->keys();
        for (auto j = range.first;j!=range.second;++j) {
            const auto& key_tmp =*j;
            const std::string key(key_tmp); // hack
            document::document_view_t view(doc);
            auto condishane = view.is_null(key) && (view.is_string(key) || view.is_long(key) || view.is_ulong(key));
            if(condishane){
                index->insert(key_tmp,doc);
            }
        }

    }

    auto search_index(const index_engine_ptr& ptr, id_index id) -> index_t* {
        return ptr->find(id);
    }

    auto make_index_engine(actor_zeta::detail::pmr::memory_resource* resource) -> index_engine_ptr {
        auto size = sizeof(index_engine_t);
        auto align = alignof(index_engine_t);
        auto* buffer = resource->allocate(size, align);
        auto* index_engine = new (buffer) index_engine_t(resource);
        return {index_engine, deleter(resource)};
    }

    index_engine_t::index_engine_t(actor_zeta::detail::pmr::memory_resource* resource)
        : resource_(resource)
        , mapper_(resource)
        , index_to_mapper_(resource)
        , storgae_(resource){
    }

    auto index_engine_t::emplace(const keys_base_t& keys, value_t value) -> uint32_t {
        auto d = storgae_.insert(storgae_.cend(), std::move(value));
        auto result = mapper_.emplace(keys, d);
        auto new_id = index_to_mapper_.size();
        index_to_mapper_.emplace(new_id, d);
        return new_id;
    }

    actor_zeta::detail::pmr::memory_resource* index_engine_t::resource() noexcept {
        return resource_;
    }

    auto index_engine_t::find(id_index id) -> index_raw_ptr {
        return index_to_mapper_.find(id)->second->get();
    }

    auto index_engine_t::size() const -> std::size_t {
        return mapper_.size();
    }

    deleter::deleter(actor_zeta::detail::pmr::memory_resource* ptr)
        : ptr_(ptr) {}

    index_t::index_t(actor_zeta::detail::pmr::memory_resource* resource,const keys_base_t& keys)
        : resource_(resource)
        , keys_(keys){}

    void index_t::find(query_t query,result_set_t*set)  {
        return find_impl(std::move(query),set);
    }

    auto index_t::insert(key_t key, value_t value) -> void  {
        return insert_impl(key, value);
    }

    index_t::~index_t() = default;
} // namespace components::index
