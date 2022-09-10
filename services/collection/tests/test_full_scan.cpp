#include <catch2/catch.hpp>
#include <components/ql/parser.hpp>
#include <components/tests/generaty.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/database/database.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/full_scan.hpp>
#include <services/collection/operators/predicates/predicate.hpp>

using namespace services::collection;
using namespace services::database;

struct context_t final  {
    using collection_ptr = actor_zeta::intrusive_ptr<collection_t>;

    collection_t* operator->() const noexcept {
        return collection_.get();
    }

    collection_t& operator*() const noexcept {
        return *(collection_);
    }

    ~context_t() {}

    actor_zeta::scheduler_ptr scheduler_;
    actor_zeta::detail::pmr::memory_resource *resource;
    std::unique_ptr<manager_database_t> manager_database_;
    std::unique_ptr<database_t> database_;
    std::unique_ptr<collection_t> collection_;
};


using context_ptr = std::unique_ptr<context_t>;

context_ptr make_context(log_t& log) {
    auto context = std::make_unique<context_t>();
    context->scheduler_.reset(new core::non_thread_scheduler::scheduler_test_t(1, 1));
    context->resource = actor_zeta::detail::pmr::get_default_resource();
    context->manager_database_ = actor_zeta::spawn_supervisor<manager_database_t>(context->resource,context->scheduler_.get(),log);
    context->database_ = actor_zeta::spawn_supervisor<database_t>(context->manager_database_.get(),"TestDataBase",log,1,1000);

    auto allocate_byte = sizeof(collection_t);
    auto allocate_byte_alignof = alignof(collection_t);
    void* buffer = context->resource->allocate(allocate_byte, allocate_byte_alignof);
    auto* collection = new (buffer) collection_t(context->database_.get(), "TestCollection", log, actor_zeta::address_t::empty_address());
    context->collection_.reset(collection);
    return context;
}

collection_t* d(context_ptr& ptr){
    return ptr->collection_.get();
}

components::ql::find_statement parse_find_condition(const std::string& cond, bool find_one = false) {
    auto expr = components::ql::parse_find_condition(components::document::document_from_json(cond));
    return components::ql::find_statement(database_name_t(), collection_name_t(), std::move(expr), find_one);
}


TEST_CASE("full_scan") {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto collection = make_context(log);

    std::list<document_ptr> documents;
    for (int i = 1; i <= 100; ++i) {
        documents.push_back(gen_doc(i));
    }
    services::collection::operators::operator_insert insert(d(collection)->view(), std::move(documents));
    insert.on_execute(nullptr);
    REQUIRE(d(collection)->size_test() == 100);

    SECTION("find::eq") {
        auto cond = parse_find_condition(R"({"count": {"$eq": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t::unlimit());
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = parse_find_condition(R"({"count": {"$ne": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t::unlimit());
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t::unlimit());
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = parse_find_condition(R"({"count": {"$gte": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t::unlimit());
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = parse_find_condition(R"({"count": {"$lt": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t::unlimit());
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = parse_find_condition(R"({"count": {"$lte": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t::unlimit());
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 90);
    }

    SECTION("find_one") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        services::collection::operators::full_scan scan(d(collection)->view(),
                                                        services::collection::operators::predicates::create_predicate(d(collection)->view(), cond),
                                                        services::collection::operators::predicates::limit_t(1));
        auto cursor = std::make_unique<components::cursor::sub_cursor_t>(d(collection)->view()->resource(), d(collection)->address());
        scan.on_execute(cursor.get());
        REQUIRE(cursor->size() == 1);
    }
}
