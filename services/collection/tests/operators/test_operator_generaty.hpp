#pragma once

#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <components/ql/parser.hpp>
#include <components/tests/generaty.hpp>
#include <services/database/database.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/operators/operator_insert.hpp>

using namespace services::collection;
using namespace services::database;

struct context_t final {
    using collection_ptr = actor_zeta::intrusive_ptr<collection_t>;

    collection_t* operator->() const noexcept {
        return collection_.get();
    }

    collection_t& operator*() const noexcept {
        return *(collection_);
    }

    ~context_t() {}

    actor_zeta::scheduler_ptr scheduler_;
    actor_zeta::detail::pmr::memory_resource* resource;
    std::unique_ptr<manager_database_t> manager_database_;
    std::unique_ptr<database_t> database_;
    std::unique_ptr<collection_t> collection_;
};

using context_ptr = std::unique_ptr<context_t>;

inline context_ptr make_context(log_t& log) {
    auto context = std::make_unique<context_t>();
    context->scheduler_.reset(new core::non_thread_scheduler::scheduler_test_t(1, 1));
    context->resource = actor_zeta::detail::pmr::get_default_resource();
    context->manager_database_ = actor_zeta::spawn_supervisor<manager_database_t>(context->resource, context->scheduler_.get(), log);
    context->database_ = actor_zeta::spawn_supervisor<database_t>(context->manager_database_.get(), "TestDataBase", log, 1, 1000);

    auto allocate_byte = sizeof(collection_t);
    auto allocate_byte_alignof = alignof(collection_t);
    void* buffer = context->resource->allocate(allocate_byte, allocate_byte_alignof);
    auto* collection = new (buffer) collection_t(context->database_.get(), "TestCollection", log, actor_zeta::address_t::empty_address());
    context->collection_.reset(collection);
    return context;
}

inline collection_t* d(context_ptr& ptr) {
    return ptr->collection_.get();
}

inline components::ql::expr_ptr parse_expr(const std::string& cond) {
    return components::ql::parse_find_condition(components::document::document_from_json(cond));
}

inline components::ql::find_statement_ptr parse_find_condition(const std::string& cond, bool find_one = false) {
    return components::ql::make_find_statement(database_name_t(), collection_name_t(), parse_expr(cond), find_one);
}

inline context_ptr create_collection() {
    static auto log = initialization_logger("python", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    return make_context(log);
}

inline void fill_collection(context_ptr &collection) {
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
