#include <catch2/catch.hpp>
#include <components/ql/parser.hpp>
#include <components/tests/generaty.hpp>
#include <components/index/single_field_index.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/database/database.hpp>
#include <services/collection/collection.hpp>
#include <services/collection/operators/full_scan.hpp>
#include <services/collection/operators/index_scan.hpp>
#include <services/collection/operators/operator_insert.hpp>
#include <services/collection/operators/operator_delete.hpp>
#include <services/collection/operators/operator_update.hpp>
#include <services/collection/operators/predicates/predicate.hpp>

using namespace services::collection;
using namespace services::database;
using namespace services::collection::operators;

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

context_ptr make_context(log_t& log) {
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

collection_t* d(context_ptr& ptr) {
    return ptr->collection_.get();
}

components::ql::expr_ptr parse_expr(const std::string& cond) {
    return components::ql::parse_find_condition(components::document::document_from_json(cond));
}

components::ql::find_statement parse_find_condition(const std::string& cond, bool find_one = false) {
    return components::ql::find_statement(database_name_t(), collection_name_t(), parse_expr(cond), find_one);
}

context_ptr create_collection() {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    return make_context(log);
}

void fill_collection(context_ptr &collection) {
    std::pmr::vector<document_ptr> documents(collection->resource);
    documents.reserve(100);
    for (int i = 1; i <= 100; ++i) {
        documents.emplace_back(gen_doc(i));
    }
    operator_insert insert(d(collection)->view(), std::move(documents));
    insert.on_execute(nullptr);
}

context_ptr init_collection() {
    auto collection = create_collection();
    fill_collection(collection);
    return collection;
}

TEST_CASE("operator::insert") {
    auto collection = init_collection();
    REQUIRE(d(collection)->size_test() == 100);
}

TEST_CASE("operator::full_scan") {
    auto collection = init_collection();

    SECTION("find::eq") {
        auto cond = parse_find_condition(R"({"count": {"$eq": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        auto cond = parse_find_condition(R"({"count": {"$ne": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        auto cond = parse_find_condition(R"({"count": {"$gte": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        auto cond = parse_find_condition(R"({"count": {"$lt": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        auto cond = parse_find_condition(R"({"count": {"$lte": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        full_scan scan(d(collection)->view(),
                       predicates::create_predicate(d(collection)->view(), cond),
                       predicates::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }
}

TEST_CASE("operator::delete") {
    auto collection = init_collection();

    SECTION("find::delete") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::unlimit()));
        delete_.on_execute(nullptr);
        REQUIRE(d(collection)->size_test() == 90);
    }

    SECTION("find::delete_one") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(1)));
        delete_.on_execute(nullptr);
        REQUIRE(d(collection)->size_test() == 99);
    }

    SECTION("find::delete_limit") {
        REQUIRE(d(collection)->size_test() == 100);
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        operator_delete delete_(d(collection)->view());
        delete_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(5)));
        delete_.on_execute(nullptr);
        REQUIRE(d(collection)->size_test() == 95);
    }
}


TEST_CASE("operator::update") {
    auto collection = init_collection();

    SECTION("find::update") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        auto cond_check = parse_find_condition(R"({"count": {"$eq": 999}})");
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t::unlimit()));
        update_.on_execute(nullptr);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("find::update_one") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        auto cond_check = parse_find_condition(R"({"count": {"$eq": 999}})");
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(1)));
        update_.on_execute(nullptr);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 1);
        }
    }

    SECTION("find::update_limit") {
        auto cond = parse_find_condition(R"({"count": {"$gt": 90}})");
        auto cond_check = parse_find_condition(R"({"count": {"$eq": 999}})");
        auto script_update = components::document::document_from_json(R"({"$set": {"count": 999}})");
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }

        operator_update update_(d(collection)->view(), std::move(script_update));
        update_.set_children(std::make_unique<full_scan>(d(collection)->view(),
                                                         predicates::create_predicate(d(collection)->view(), cond),
                                                         predicates::limit_t(5)));
        update_.on_execute(nullptr);
        {
            full_scan scan(d(collection)->view(),
                           predicates::create_predicate(d(collection)->view(), cond_check),
                           predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 5);
        }
    }
}

TEST_CASE("operator::index_scan") {
    auto collection = create_collection();
    components::index::keys_base_storage_t keys(collection->resource);
    keys.push_back(components::index::key_t("count"));
    components::index::make_index<components::index::single_field_index_t>(d(collection)->view()->index_engine(), keys);
    fill_collection(collection);

    SECTION("find::eq") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$eq": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find::ne") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$ne": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 99);
    }

    SECTION("find::gt") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 10);
    }

    SECTION("find::gte") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gte": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 11);
    }

    SECTION("find::lt") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$lt": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 89);
    }

    SECTION("find::lte") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$lte": 90}})"), predicates::limit_t::unlimit());
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 90);
    }

    SECTION("find_one") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 90}})"), predicates::limit_t(1));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 1);
    }

    SECTION("find_limit") {
        index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 90}})"), predicates::limit_t(3));
        scan.on_execute(nullptr);
        REQUIRE(scan.output()->size() == 3);
    }
}

TEST_CASE("operator::index::delete_and_update") {
    auto collection = create_collection();
    components::index::keys_base_storage_t keys(collection->resource);
    keys.push_back(components::index::key_t("count"));
    components::index::make_index<components::index::single_field_index_t>(d(collection)->view()->index_engine(), keys);
    fill_collection(collection);

    SECTION("index_scan after delete") {
        {
            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 50);
        }
        {
            operator_delete delete_(d(collection)->view());
            delete_.set_children(std::make_unique<index_scan>(d(collection)->view(),
                                                              parse_expr(R"({"count": {"$gt": 60}})"), predicates::limit_t::unlimit()));
            delete_.on_execute(nullptr);

            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$gt": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 10);
        }
    }

    SECTION("index_scan after update") {
        {
            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$eq": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 1);
        }
        {
            auto script_update = components::document::document_from_json(R"({"$set": {"count": 0}})");
            operator_update update(d(collection)->view(), script_update);
            update.set_children(std::make_unique<index_scan>(d(collection)->view(),
                                                             parse_expr(R"({"count": {"$eq": 50}})"), predicates::limit_t::unlimit()));
            update.on_execute(nullptr);

            index_scan scan(d(collection)->view(), parse_expr(R"({"count": {"$eq": 50}})"), predicates::limit_t::unlimit());
            scan.on_execute(nullptr);
            REQUIRE(scan.output()->size() == 0);
        }
    }
}
