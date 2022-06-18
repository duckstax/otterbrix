#include "index.hpp"

namespace components::index {

    void find(const index_engine_ptr& index, keys_base_t params, query_t query) {
        search_index(index, std::move(params))->find(query);
    }

    void insert(const index_engine_ptr& ptr, keys_base_t params, std::vector<document_ptr> docs) {
        auto * index = search_index(ptr, std::move(params));
        for(const auto&i:docs) {
            index->insert(docs);
        }
    }

    auto search_index(const index_engine_ptr& ptr, keys_base_t keys) -> index_t* {
        return ptr->find(std::move(keys));
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

    auto index_engine_t::emplace(keys_base_t keys, index_engine_t::value_t value) -> void {
        mapper_.emplace(std::move(keys), std::move(value));
    }

    actor_zeta::detail::pmr::memory_resource* index_engine_t::resource() noexcept {
        return resource_;
    }

    auto index_engine_t::find(keys_base_t keys) -> index_raw_ptr {
        const auto tmp_keys = std::move(keys);
        auto it = mapper_.find(tmp_keys);
        return it->second.get();
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
    auto index_t::insert() {
        return insert_impl();
    }
    index_t::~index_t() = default;
} // namespace components::index
