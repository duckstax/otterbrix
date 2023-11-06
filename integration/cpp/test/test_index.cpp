#include <catch2/catch.hpp>
#include "test_config.hpp"

using components::ql::aggregate::operator_type;
using components::expressions::compare_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";


#define INIT_COLLECTION() \
    do { \
        { \
            auto session = ottergon::session_id_t(); \
            dispatcher->create_database(session, database_name); \
        } \
        { \
            auto session = ottergon::session_id_t(); \
            dispatcher->create_collection(session, database_name, collection_name); \
        } \
    } while (false)

#define FILL_COLLECTION() \
    do { \
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource()); \
        for (int num = 1; num <= 100; ++num) { \
            documents.push_back(gen_doc(num)); \
        } \
        { \
            auto session = ottergon::session_id_t(); \
            dispatcher->insert_many(session, database_name, collection_name, documents); \
        } \
    } while (false)

#define CREATE_INDEX(INDEX_COMPARE, KEY) \
    do { \
        auto session = ottergon::session_id_t(); \
        components::ql::create_index_t ql{database_name, collection_name, components::ql::index_type::single, INDEX_COMPARE}; \
        ql.keys_.emplace_back(KEY); \
        dispatcher->create_index(session, ql); \
    } while (false)

#define DROP_INDEX(KEY) \
    do { \
        auto session = ottergon::session_id_t(); \
        components::ql::drop_index_t ql{database_name, collection_name}; \
        ql.keys_.emplace_back(KEY); \
        dispatcher->drop_index(session, ql); \
    } while (false)

#define CHECK_FIND_ALL() \
    do { \
        auto session = ottergon::session_id_t(); \
        auto *ql = new components::ql::aggregate_statement{database_name, collection_name}; \
        auto c = dispatcher->find(session, ql).get<components::cursor::cursor_t*>(); \
        REQUIRE(c->size() == 100); \
        delete c; \
    } while (false)

#define CHECK_FIND(KEY, COMPARE, VALUE, COUNT) \
    do { \
        auto session = ottergon::session_id_t(); \
        auto *ql = new components::ql::aggregate_statement{database_name, collection_name}; \
        auto expr = components::expressions::make_compare_expression(dispatcher->resource(), COMPARE, key{KEY}, id_par{1}); \
        ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr))); \
        ql->add_parameter(id_par{1}, VALUE); \
        auto c = dispatcher->find(session, ql).get<components::cursor::cursor_t*>(); \
        REQUIRE(c->size() == COUNT); \
        delete c; \
    } while (false)

#define CHECK_FIND_COUNT(COMPARE, VALUE, COUNT) \
    CHECK_FIND("count", COMPARE, VALUE, COUNT)

#define CHECK_EXISTS_INDEX(NAME, EXISTS) \
    do { \
        auto index_name = collection_name + "__" + NAME; \
        auto path = config.disk.path / "indexes" / collection_name / index_name; \
        REQUIRE(std::filesystem::exists(path) == EXISTS); \
        REQUIRE(std::filesystem::is_directory(path) == EXISTS); \
    } while (false)



TEST_CASE("integration::test_index::base") {
    auto config = test_create_config("/tmp/ottergon/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX(components::ql::index_compare::int64, "count");
        FILL_COLLECTION();
    }

    INFO("find") {
        CHECK_FIND_ALL();
        CHECK_FIND_COUNT(compare_type::eq, 10, 1);
        CHECK_FIND_COUNT(compare_type::gt, 10, 90);
        CHECK_FIND_COUNT(compare_type::lt, 10, 9);
        CHECK_FIND_COUNT(compare_type::ne, 10, 99);
        CHECK_FIND_COUNT(compare_type::gte, 10, 91);
        CHECK_FIND_COUNT(compare_type::lte, 10, 10);
    }
}


TEST_CASE("integration::test_index::save_load") {
    auto config = test_create_config("/tmp/ottergon/integration/test_index/save_load");
    test_clear_directory(config);

    INFO("initialization") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        
        INIT_COLLECTION();
        CREATE_INDEX(components::ql::index_compare::int64, "count");
        CREATE_INDEX(components::ql::index_compare::str, "countStr");
        CREATE_INDEX(components::ql::index_compare::float64, "countDouble");
        FILL_COLLECTION();
    }

    INFO("find") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        dispatcher->load();

        CHECK_FIND_ALL();
        CHECK_FIND_COUNT(compare_type::eq, 10, 1);
        CHECK_FIND_COUNT(compare_type::gt, 10, 90);
        CHECK_FIND_COUNT(compare_type::lt, 10, 9);
        CHECK_FIND_COUNT(compare_type::ne, 10, 99);
        CHECK_FIND_COUNT(compare_type::gte, 10, 91);
        CHECK_FIND_COUNT(compare_type::lte, 10, 10);
    }
}


TEST_CASE("integration::test_index::drop") {
    auto config = test_create_config("/tmp/ottergon/integration/test_index/drop");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX(components::ql::index_compare::int64, "count");
        CREATE_INDEX(components::ql::index_compare::str, "countStr");
        CREATE_INDEX(components::ql::index_compare::float64, "countDouble");
        FILL_COLLECTION();
        usleep(1000000); //todo: wait
    }

    INFO("drop indexes") {
        CHECK_EXISTS_INDEX("count", true);
        CHECK_EXISTS_INDEX("countStr", true);
        CHECK_EXISTS_INDEX("countDouble", true);

        DROP_INDEX("count");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("count", false);
        CHECK_EXISTS_INDEX("countStr", true);
        CHECK_EXISTS_INDEX("countDouble", true);

        DROP_INDEX("countStr");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("count", false);
        CHECK_EXISTS_INDEX("countStr", false);
        CHECK_EXISTS_INDEX("countDouble", true);

        DROP_INDEX("countDouble");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("count", false);
        CHECK_EXISTS_INDEX("countStr", false);
        CHECK_EXISTS_INDEX("countDouble", false);

        DROP_INDEX("count");
        DROP_INDEX("count");
        DROP_INDEX("count");
        DROP_INDEX("count");
        DROP_INDEX("count");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("count", false);
        CHECK_EXISTS_INDEX("countStr", false);
        CHECK_EXISTS_INDEX("countDouble", false);
    }
}
