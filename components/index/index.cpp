#include "index.hpp"

#include <utility>

#include "document/mutable/mutable_dict.hpp"

namespace components::index {

    auto find(const index_engine_ptr& index, const keys_base_t& params, query_t query) {
        return search_index(index, params)->find(std::move(query));
    }

    void insert(const index_engine_ptr& ptr, keys_base_t params, std::vector<document_ptr>& docs) {
        auto* index = search_index(ptr, std::move(params));
        for (const auto& i : docs) {
            for (auto& j : params) {
                /// index->insert(i->structure->get(i));
            }
        }
    }

    void insert_one(const index_engine_ptr& ptr, const keys_base_t& params, document_ptr docs) {
        auto* index = search_index(ptr, params);
        ///   index->insert(i->structure->get(i));
    }

    auto search_index(const index_engine_ptr& ptr, const keys_base_t& keys) -> index_t* {
        return ptr->find(keys);
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
        , mapper_(resource) {
    }

    auto index_engine_t::emplace(keys_base_t keys, value_t value) -> uint32_t {
        auto d = storgae_.insert(storgae_.cend(), wrapper(keys,std::move(value)));

        auto result = mapper_.emplace(std::move(keys), d);
        auto new_id = index_to_mapper_.size();
        index_to_mapper_.emplace(new_id, d);
        return new_id;
    }

    actor_zeta::detail::pmr::memory_resource* index_engine_t::resource() noexcept {
        return resource_;
    }

    auto index_engine_t::find(const keys_base_t& keys) -> index_raw_ptr {
        auto it = mapper_.find(keys);
        ///return it->second.get();
    }

    auto index_engine_t::size() const -> std::size_t {
        return mapper_.size();
    }

    deleter::deleter(actor_zeta::detail::pmr::memory_resource* ptr)
        : ptr_(ptr) {}

    index_t::index_t(actor_zeta::detail::pmr::memory_resource* resource)
        : resource_(resource) {}

    auto index_t::find(query_t query) -> result_set_t {
        return find_impl(query);
    }
    auto index_t::insert(key_t key, value_t value) {
        return insert_impl(key, value);
    }
    index_t::~index_t() = default;
} // namespace components::index
