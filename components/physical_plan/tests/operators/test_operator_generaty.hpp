#pragma once

#include <components/physical_plan/base/operators/operator_raw_data.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/tests/generaty.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/collection/collection.hpp>
#include <services/memory_storage/memory_storage.hpp>

using namespace services;
using namespace services::collection;

struct context_t final {
    context_t(log_t& log, std::pmr::memory_resource* resource)
        : resource_(resource)
        , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
        , memory_storage_(actor_zeta::spawn_supervisor<memory_storage_t>(resource_, scheduler_.get(), log))
        , collection_([this](auto& log) {
            collection_full_name_t name;
            name.database = "TestDatabase";
            name.collection = "TestCollection";
            auto allocate_byte = sizeof(context_collection_t);
            auto allocate_byte_alignof = alignof(context_collection_t);
            void* buffer = resource_->allocate(allocate_byte, allocate_byte_alignof);
            auto* collection =
                new (buffer) context_collection_t(resource_, name, actor_zeta::address_t::empty_address(), log);
            return std::unique_ptr<context_collection_t, actor_zeta::pmr::deleter_t>(
                collection,
                actor_zeta::pmr::deleter_t(resource_));
        }(log)) {}

    ~context_t() = default;

    std::pmr::memory_resource* resource_;
    actor_zeta::scheduler_ptr scheduler_;
    std::unique_ptr<memory_storage_t, actor_zeta::pmr::deleter_t> memory_storage_;
    std::unique_ptr<context_collection_t, actor_zeta::pmr::deleter_t> collection_;
};

using context_ptr = std::unique_ptr<context_t>;

inline context_ptr make_context(log_t& log, std::pmr::memory_resource* resource) {
    return std::make_unique<context_t>(log, resource);
}

inline context_collection_t* d(context_ptr& ptr) { return ptr->collection_.get(); }

inline context_ptr create_collection(std::pmr::memory_resource* resource) {
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    return make_context(log, resource);
}

inline void fill_collection(context_ptr& collection) {
    std::pmr::vector<document_ptr> documents(collection->resource_);
    documents.reserve(100);
    for (int i = 1; i <= 100; ++i) {
        documents.emplace_back(gen_doc(i, collection->resource_));
    }
    operators::operator_insert insert(collection->collection_.get());
    insert.set_children({new base::operators::operator_raw_data_t(std::move(documents))});
    insert.on_execute(nullptr);
}

inline context_ptr init_collection(std::pmr::memory_resource* resource) {
    auto collection = create_collection(resource);
    fill_collection(collection);
    return collection;
}
