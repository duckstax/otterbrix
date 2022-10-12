#include <catch2/catch.hpp>
#include "test_config.hpp"

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";

TEST_CASE("duck_charmer::test_collection") {
    auto config = test_create_config("/tmp/test_collection");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    dispatcher->load();

    INFO("initialization") {
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_database(session, database_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->create_collection(session, database_name, collection_name);
        }
        {
            auto session = duck_charmer::session_id_t();
            REQUIRE(*dispatcher->size(session, database_name, collection_name) == 0);
        }
    }

    INFO("one_insert") {
        for (int num = 0; num < 50; ++num) {
            {
                auto doc = gen_doc(num);
                auto session = duck_charmer::session_id_t();
                dispatcher->insert_one(session, database_name, collection_name, doc);
            }
            {
                auto session = duck_charmer::session_id_t();
                REQUIRE(*dispatcher->size(session, database_name, collection_name) == static_cast<std::size_t>(num) + 1);
            }
        }
        auto session = duck_charmer::session_id_t();
        REQUIRE(*dispatcher->size(session, database_name, collection_name) == 50);
    }

    INFO("many_insert") {
        std::list<components::document::document_ptr> documents;
        for (int num = 50; num < 100; ++num) {
            documents.push_back(gen_doc(num));
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->insert_many(session, database_name, collection_name, documents);
        }
        {
            auto session = duck_charmer::session_id_t();
            REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
        }
    }

    INFO("insert non unique id") {
        for (int num = 0; num < 100; ++num) {
            {
                auto doc = gen_doc(num);
                auto session = duck_charmer::session_id_t();
                dispatcher->insert_one(session, database_name, collection_name, doc);
            }
            {
                auto session = duck_charmer::session_id_t();
                REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
            }
        }
        auto session = duck_charmer::session_id_t();
        REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
    }

    INFO("find") {
        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find(session, database_name, collection_name, make_document());
            REQUIRE(c->size() == 100);
            delete c;
        }

        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find(session, database_name, collection_name, make_condition("count", "$gt", 90));
            REQUIRE(c->size() == 9);
            delete c;
        }

        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find(session, database_name, collection_name, make_condition("countStr", "$regex", std::string("9$")));
            REQUIRE(c->size() == 10);
            delete c;
        }

        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find(session, database_name, collection_name, make_condition("$or", {
                                                                                                         make_dict("count", "$gt", 90),
                                                                                                         make_dict("countStr", "$regex", std::string("9$"))
                                                                                                     }));
            REQUIRE(c->size() == 18);
            delete c;
        }

        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find(session, database_name, collection_name, make_condition("$and", {
                                                                                                        make_dict("$or", {
                                                                                                            make_dict("count", "$gt", 90),
                                                                                                            make_dict("countStr", "$regex", std::string("9$"))
                                                                                                        }),
                                                                                                        make_dict("count", "$lte", 30)
                                                                                                      }));
            REQUIRE(c->size() == 3);
            delete c;
        }
    }

    INFO("cursor") {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_document());
        REQUIRE(c->size() == 100);
        int count = 0;
        while (c->has_next()) {
            c->next();
            ++count;
        }
        REQUIRE(count == 100);
        delete c;
    }

    INFO("find_one") {
        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find_one(session, database_name, collection_name, make_condition("_id", "$eq", gen_id(1)));
            REQUIRE(c->get_long("count") == 1);
        }
        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find_one(session, database_name, collection_name, make_condition("count", "$eq", 10));
            REQUIRE(c->get_long("count") == 10);
        }
        {
            auto session = duck_charmer::session_id_t();
            auto c = dispatcher->find_one(session, database_name, collection_name, make_condition("$and", {
                                                                                                             make_dict("count", "$gt", 90),
                                                                                                             make_dict("countStr", "$regex", std::string("9$"))
                                                                                                         }));
            REQUIRE(c->get_long("count") == 99);
        }
    }
}
