#include <catch2/catch.hpp>
#include <apps/duck_charmer/spaces.hpp>
#include <components/document/document.hpp>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/mutable/mutable_array.h>
#include <components/protocol/base.hpp>

static const database_name_t database_name = "FriedrichDatabase";
static const collection_name_t collection_name = "FriedrichCollection";

TEST_CASE("duck_charmer::test_collection") {
    auto *space = duck_charmer::spaces::get_instance();
    auto *dispatcher = space->dispatcher();
    auto session = duck_charmer::session_id_t();

    SECTION("initialization") {
        REQUIRE(true);
        dispatcher->create_database(session, database_name);
        dispatcher->create_collection(session, database_name, collection_name);
        REQUIRE(*dispatcher->size(session, database_name, collection_name) == 0);
    }

    SECTION("one_insert") {
        for (size_t num = 0; num < 50; ++num) {
            auto new_obj = document::impl::mutable_dict_t::new_dict();
            new_obj->set("_id", std::to_string(num));
            new_obj->set("count", num);
            new_obj->set("countStr", std::to_string(num));
            new_obj->set("countFloat", float(num) + 0.1);
            new_obj->set("countBool", (num & 1) != 0);
            auto array = document::impl::mutable_array_t::new_array();
            for (size_t i = 0; i < 5; ++i) {
                array->append(num + i);
            }
            new_obj->set("countArray", array);
            auto dict = document::impl::mutable_dict_t::new_dict();
            dict->set("odd", (num & 1) != 0);
            dict->set("even", (num & 1) == 0);
            dict->set("three", (num % 3) == 0);
            dict->set("five", (num % 5) == 0);
            new_obj->set("countDict", dict);

            auto doc = components::document::make_document(new_obj);
            dispatcher->insert_one(session, database_name, collection_name, doc);
            REQUIRE(*dispatcher->size(session, database_name, collection_name) == num + 1);
        }
        REQUIRE(*dispatcher->size(session, database_name, collection_name) == 50);
    }

    SECTION("many_insert") {
        std::list<components::document::document_ptr> documents;
        for (size_t num = 50; num < 100; ++num) {
            auto new_obj = document::impl::mutable_dict_t::new_dict();
            new_obj->set("_id", std::to_string(num));
            new_obj->set("count", num);
            new_obj->set("countStr", std::to_string(num));
            new_obj->set("countFloat", float(num) + 0.1);
            new_obj->set("countBool", (num & 1) != 0);
            auto array = document::impl::mutable_array_t::new_array();
            for (size_t i = 0; i < 5; ++i) {
                array->append(num + i);
            }
            new_obj->set("countArray", array);
            auto dict = document::impl::mutable_dict_t::new_dict();
            dict->set("odd", (num & 1) != 0);
            dict->set("even", (num & 1) == 0);
            dict->set("three", (num % 3) == 0);
            dict->set("five", (num % 5) == 0);
            new_obj->set("countDict", dict);

            documents.push_back(components::document::make_document(new_obj));
        }
        dispatcher->insert_many(session, database_name, collection_name, documents);
        REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
    }

    SECTION("insert non unique id") {
        for (size_t num = 0; num < 100; ++num) {
            auto new_obj = document::impl::mutable_dict_t::new_dict();
            new_obj->set("_id", std::to_string(num));
            auto doc = components::document::make_document(new_obj);
            dispatcher->insert_one(session, database_name, collection_name, doc);
            REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
        }
        REQUIRE(*dispatcher->size(session, database_name, collection_name) == 100);
    }
}
