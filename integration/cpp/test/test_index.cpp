#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <unistd.h>

using components::expressions::compare_type;
using components::ql::aggregate::operator_type;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

constexpr int kDocuments = 100;

#define INIT_COLLECTION()                                                                                              \
    do {                                                                                                               \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_database(session, database_name);                                                       \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->create_collection(session, database_name, collection_name);                                    \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION()                                                                                              \
    do {                                                                                                               \
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());                        \
        for (int num = 1; num <= kDocuments; ++num) {                                                                  \
            documents.push_back(gen_doc(num));                                                                         \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            dispatcher->insert_many(session, database_name, collection_name, documents);                               \
        }                                                                                                              \
    } while (false)

#define FILL_COLLECTION_INSERT_ONE()                                                                                   \
    do {                                                                                                               \
        std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());                        \
        for (int num = 1; num <= kDocuments; ++num) {                                                                  \
            documents.push_back(gen_doc(num));                                                                         \
        }                                                                                                              \
        {                                                                                                              \
            auto session = otterbrix::session_id_t();                                                                  \
            for (size_t num = 0; num < documents.size(); ++num) {                                                      \
                dispatcher->insert_one(session, database_name, collection_name, documents.at(num));                    \
            }                                                                                                          \
        }                                                                                                              \
    } while (false)

#define CREATE_INDEX(INDEX_NAME, INDEX_COMPARE, KEY)                                                                   \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        components::ql::create_index_t ql{database_name,                                                               \
                                          collection_name,                                                             \
                                          INDEX_NAME,                                                                  \
                                          components::ql::index_type::single,                                          \
                                          INDEX_COMPARE};                                                              \
        ql.keys_.emplace_back(KEY);                                                                                    \
        dispatcher->create_index(session, &ql);                                                                        \
    } while (false)

#define CREATE_EXISTED_INDEX(INDEX_NAME, INDEX_COMPARE, KEY)                                                           \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        components::ql::create_index_t ql{database_name,                                                               \
                                          collection_name,                                                             \
                                          INDEX_NAME,                                                                  \
                                          components::ql::index_type::single,                                          \
                                          INDEX_COMPARE};                                                              \
        ql.keys_.emplace_back(KEY);                                                                                    \
        auto res = dispatcher->create_index(session, &ql);                                                             \
        REQUIRE(res->is_error() == true);                                                                              \
        REQUIRE(res->get_error().type == components::cursor::error_code_t::index_create_fail);                         \
                                                                                                                       \
    } while (false)

#define DROP_INDEX(INDEX_NAME)                                                                                         \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        components::ql::drop_index_t ql{database_name, collection_name, INDEX_NAME};                                   \
        dispatcher->drop_index(session, &ql);                                                                          \
    } while (false)

#define CHECK_FIND_ALL()                                                                                               \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto* ql = new components::ql::aggregate_statement{database_name, collection_name};                            \
        auto c = dispatcher->find(session, ql);                                                                        \
        REQUIRE(c->size() == kDocuments);                                                                              \
    } while (false)

#define CHECK_FIND(KEY, COMPARE, VALUE, COUNT)                                                                         \
    do {                                                                                                               \
        auto session = otterbrix::session_id_t();                                                                      \
        auto* ql = new components::ql::aggregate_statement{database_name, collection_name};                            \
        auto expr =                                                                                                    \
            components::expressions::make_compare_expression(dispatcher->resource(), COMPARE, key{KEY}, id_par{1});    \
        ql->append(operator_type::match, components::ql::aggregate::make_match(std::move(expr)));                      \
        ql->add_parameter(id_par{1}, VALUE);                                                                           \
        auto c = dispatcher->find(session, ql);                                                                        \
        REQUIRE(c->size() == COUNT);                                                                                   \
    } while (false)

#define CHECK_FIND_COUNT(COMPARE, VALUE, COUNT) CHECK_FIND("count", COMPARE, VALUE, COUNT)

#define CHECK_EXISTS_INDEX(NAME, EXISTS)                                                                               \
    do {                                                                                                               \
        auto index_name = collection_name + "_" + NAME;                                                                \
        auto path = config.disk.path / "indexes" / collection_name / index_name;                                       \
        REQUIRE(std::filesystem::exists(path) == EXISTS);                                                              \
        REQUIRE(std::filesystem::is_directory(path) == EXISTS);                                                        \
    } while (false)

TEST_CASE("integration::test_index::base") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", core::type::int64, "count");
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
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/save_load");
    test_clear_directory(config);

    INFO("initialization") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("ncount", core::type::int64, "count");
        CREATE_INDEX("scount", core::type::str, "countStr");
        CREATE_INDEX("dcount", core::type::float64, "countDouble");
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
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/drop");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", core::type::int64, "count");
        CREATE_INDEX("scount", core::type::str, "countStr");
        CREATE_INDEX("dcount", core::type::float64, "countDouble");
        FILL_COLLECTION();
        usleep(1000000); //todo: wait
    }

    INFO("drop indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("ncount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("scount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", true);

        DROP_INDEX("dcount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", false);

        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        DROP_INDEX("ncount");
        usleep(100000); //todo: wait
        CHECK_EXISTS_INDEX("ncount", false);
        CHECK_EXISTS_INDEX("scount", false);
        CHECK_EXISTS_INDEX("dcount", false);
    }
}

TEST_CASE("integration::test_index::index already exist") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", core::type::int64, "count");
        CREATE_INDEX("scount", core::type::str, "countStr");
        CREATE_INDEX("dcount", core::type::float64, "countDouble");
        FILL_COLLECTION();
    }

    INFO("add existed ncount index") {
        CREATE_EXISTED_INDEX("ncount", core::type::int64, "count");
        CREATE_EXISTED_INDEX("ncount", core::type::int64, "count");
    }

    INFO("add existed scount index") {
        CREATE_INDEX("scount", core::type::str, "countStr");
        CREATE_INDEX("scount", core::type::str, "countStr");
    }

    INFO("add existed dcount index") {
        CREATE_INDEX("dcount", core::type::float64, "countDouble");
        CREATE_INDEX("dcount", core::type::float64, "countDouble");
    }

    INFO("find") {
        CHECK_FIND_ALL();
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("scount", true);
        CHECK_EXISTS_INDEX("dcount", true);
    }
}

TEST_CASE("integration::test_index::no_type base check") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        FILL_COLLECTION();
        CREATE_INDEX("ncount", core::type::undef, "count");
        CREATE_INDEX("dcount", core::type::undef, "countDouble");
        CREATE_INDEX("scount", core::type::undef, "countStr");
    }

    INFO("check indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    // Not working
    // INFO("find"){
    // CHECK_FIND_COUNT(compare_type::eq, 10, 1);
    // CHECK_FIND_COUNT(compare_type::gt, 10, 90);
    // CHECK_FIND_COUNT(compare_type::lt, 10, 9);
    // CHECK_FIND_COUNT(compare_type::ne, 10, 99);
    // CHECK_FIND_COUNT(compare_type::gte, 10, 91);
    // CHECK_FIND_COUNT(compare_type::lte, 10, 10);
    // }
}

TEST_CASE("integration::test_index::no_type pending base check") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/base");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("initialization") {
        INIT_COLLECTION();
        CREATE_INDEX("ncount", core::type::undef, "count");
        CREATE_INDEX("dcount", core::type::undef, "countDouble");
        CREATE_INDEX("scount", core::type::undef, "countStr");
        FILL_COLLECTION();
    }

    INFO("check indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    // Not working
    // INFO("find"){
    // CHECK_FIND_COUNT(compare_type::eq, 10, 1);
    // CHECK_FIND_COUNT(compare_type::gt, 10, 90);
    // CHECK_FIND_COUNT(compare_type::lt, 10, 9);
    // CHECK_FIND_COUNT(compare_type::ne, 10, 99);
    // CHECK_FIND_COUNT(compare_type::gte, 10, 91);
    // CHECK_FIND_COUNT(compare_type::lte, 10, 10);
    // }
}

// TODO this test is unstable
TEST_CASE("integration::test_index::no_type save_load") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index/save_load");
    test_clear_directory(config);

    INFO("initialization") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        INIT_COLLECTION();
        CREATE_INDEX("ncount", core::type::undef, "count");
        CREATE_INDEX("scount", core::type::undef, "countStr");
        CREATE_INDEX("dcount", core::type::undef, "countDouble");
        FILL_COLLECTION();
    }

    INFO("check indexes") {
        CHECK_EXISTS_INDEX("ncount", true);
        CHECK_EXISTS_INDEX("dcount", true);
        CHECK_EXISTS_INDEX("scount", true);
    }

    INFO("find") {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        dispatcher->load();

        CHECK_FIND_ALL();
        // CHECK_FIND_COUNT(compare_type::eq, 10, 1);
        // CHECK_FIND_COUNT(compare_type::gt, 10, 90);
        // CHECK_FIND_COUNT(compare_type::lt, 10, 9);
        // CHECK_FIND_COUNT(compare_type::ne, 10, 99);
        // CHECK_FIND_COUNT(compare_type::gte, 10, 91);
        // CHECK_FIND_COUNT(compare_type::lte, 10, 10);
    }
}