#pragma once

#include <components/tests/generaty.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/memory_storage/memory_storage.hpp>

using namespace services;
using namespace services::collection;

struct context_t final {
    using collection_ptr = actor_zeta::intrusive_ptr<collection_t>;

    collection_t* operator->() const noexcept { return collection_.get(); }

    collection_t& operator*() const noexcept { return *(collection_); }

    ~context_t() {}

    actor_zeta::scheduler_ptr scheduler_;
    actor_zeta::detail::pmr::memory_resource* resource;
    std::unique_ptr<memory_storage_t> memory_storage_;
    std::unique_ptr<collection_t> collection_;
};

using context_ptr = std::unique_ptr<context_t>;

inline context_ptr make_context(log_t& log) {
    auto context = std::make_unique<context_t>();
    context->scheduler_.reset(new core::non_thread_scheduler::scheduler_test_t(1, 1));
    context->resource = actor_zeta::detail::pmr::get_default_resource();
    context->memory_storage_ =
        actor_zeta::spawn_supervisor<memory_storage_t>(context->resource, context->scheduler_.get(), log);

    collection_full_name_t name;
    name.database = "TestDatabase";
    name.collection = "TestCollection";
    auto allocate_byte = sizeof(collection_t);
    auto allocate_byte_alignof = alignof(collection_t);
    void* buffer = context->resource->allocate(allocate_byte, allocate_byte_alignof);
    auto* collection =
        new (buffer) collection_t(context->memory_storage_.get(), name, log, actor_zeta::address_t::empty_address());
    context->collection_.reset(collection);
    return context;
}

inline collection_t* d(context_ptr& ptr) { return ptr->collection_.get(); }

inline context_ptr create_collection() {
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    return make_context(log);
}

inline void fill_collection(context_ptr& collection) {
    std::pmr::vector<document_ptr> documents(collection->resource);
    documents.reserve(100);
    for (int i = 1; i <= 100; ++i) {
        documents.emplace_back(gen_doc(i));
    }
    services::collection::operators::operator_insert insert(d(collection)->view(), std::move(documents));
    insert.on_execute(nullptr);
}

inline context_ptr init_collection() {
    auto collection = create_collection();
    fill_collection(collection);
    return collection;
}
