#include "collection.hpp"
#include <services/database/database.hpp>
#include "document/core/array.hpp"
#include "document/mutable/mutable_array.h"
#include "parser/parser.hpp"
#include <catch2/catch.hpp>

using namespace services::storage;

document_t gen_sub_doc(const std::string& name, bool male) {
    document_t sub_doc;
    sub_doc.add_string("name", name);
    sub_doc.add_bool("male", male);
    return sub_doc;
}

document_t gen_doc(const std::string& id, const std::string& name, const std::string& type, ulong age, bool male,
                   std::initializer_list<std::string> friends, document_t sub_doc) {
    document_t doc;
    doc.add_string("_id", id);
    doc.add_string("name", name);
    doc.add_string("type", type);
    doc.add_ulong("age", age);
    doc.add_bool("male", male);
    auto array = ::document::impl::mutable_array_t::new_array();
    for (auto value : friends) {
        array->append(value);
    }
    array->append(array->copy());
    doc.add_array("friends", array);
    sub_doc.add_dict("sub", gen_sub_doc(name, male));
    doc.add_dict("sub_doc", sub_doc);

    return doc;
}

collection_ptr gen_collection() {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);

    auto manager = goblin_engineer::make_manager_service<manager_database_t>(log, 1, 1000);
    auto allocate_byte = sizeof(collection_t);
    auto allocate_byte_alignof = alignof(collection_t);
    void* buffer = manager->resource()->allocate(allocate_byte, allocate_byte_alignof);
    auto collection = new (buffer) collection_t(nullptr, "TestCollection", log);

    collection->insert_test(gen_doc("id_1", "Rex", "dog", 6, true, {"Lucy", "Charlie"}, gen_sub_doc("Lucy", true)));
    collection->insert_test(gen_doc("id_2", "Lucy", "dog", 2, false, {"Rex", "Charlie"}, gen_sub_doc("Rex", true)));
    collection->insert_test(gen_doc("id_3", "Chevi", "cat", 3, false, {"Isha"}, gen_sub_doc("Isha", true)));
    collection->insert_test(gen_doc("id_4", "Charlie", "dog", 2, true, {"Rex", "Lucy"}, gen_sub_doc("Lucy", false)));
    collection->insert_test(gen_doc("id_5", "Isha", "cat", 6, false, {"Chevi"}, gen_sub_doc("Chevi", true)));

    return collection;
}

find_condition_ptr parse_find_condition(const std::string& cond) {
    return components::parser::parse_find_condition(document_t::from_json(cond));
}

TEST_CASE("collection_t get") {
    auto collection = gen_collection();
    //std::cout << "INDEX:\n" << collection->get_index_test() << std::endl;
    //std::cout << "DATA:\n" << collection->get_data_test() << std::endl;

    REQUIRE(collection->size_test() == 5);

    auto doc1 = collection->get_test("id_1");
    //std::cout << "DOC1:\n" << doc1.to_json() << std::endl;
    REQUIRE(doc1.is_valid());
    REQUIRE(doc1.is_exists("name"));
    REQUIRE(doc1.is_exists("age"));
    REQUIRE(doc1.is_exists("male"));
    REQUIRE_FALSE(doc1.is_exists("other"));
    REQUIRE(doc1.is_string("name"));
    REQUIRE(doc1.is_long("age"));
    REQUIRE(doc1.is_bool("male"));
    REQUIRE(doc1.is_array("friends"));
    REQUIRE(doc1.is_dict("sub_doc"));
    REQUIRE(doc1.get_string("name") == "Rex");
    REQUIRE(doc1.get_long("age") == 6);
    REQUIRE(doc1.get_bool("male") == true);

    auto doc1_friends = doc1.get_array("friends");
    //std::cout << "DOC1_FRIENDS:\n" << doc1_friends.to_json() << std::endl;
    REQUIRE(doc1_friends.is_array());
    REQUIRE(doc1_friends.count() == 3);
    REQUIRE(doc1_friends.get_as<std::string>(1) == "Charlie");

    auto doc1_sub = doc1.get_dict("sub_doc");
    //std::cout << "DOC1_SUB:\n" << doc1_sub.to_json() << std::endl;
    REQUIRE(doc1_sub.is_dict());
    REQUIRE(doc1_sub.count() == 3);
    REQUIRE(doc1_sub.get_as<std::string>("name") == "Lucy");

    auto doc6 = collection->get_test("id_6");
    REQUIRE_FALSE(doc6.is_valid());
}

TEST_CASE("collection_t find") {
    auto collection = gen_collection();
    auto res = collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"));
    REQUIRE(res->size() == 1);
    res = collection->find_test(parse_find_condition("{\"age\": {\"$gt\": 2, \"$lte\": 4}}"));
    REQUIRE(res->size() == 1);
    res = collection->find_test(parse_find_condition("{\"$and\": [{\"age\": {\"$gt\": 2}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(parse_find_condition("{\"$or\": [{\"name\": {\"$eq\": \"Rex\"}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
    REQUIRE(res->size() == 3);
    res = collection->find_test(parse_find_condition("{\"$not\": {\"type\": {\"$eq\": \"cat\"}}}"));
    REQUIRE(res->size() == 3);
    res = collection->find_test(parse_find_condition("{\"type\": {\"$in\": [\"cat\",\"dog\"]}}"));
    REQUIRE(res->size() == 5);
    res = collection->find_test(parse_find_condition("{\"name\": {\"$in\": [\"Rex\",\"Lucy\",\"Tank\"]}}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(parse_find_condition("{\"name\": {\"$all\": [\"Rex\",\"Lucy\"]}}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(parse_find_condition("{\"name\": {\"$regex\": \"^Ch\"}}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(parse_find_condition("{}"));
    REQUIRE(res->size() == 5);
    res = collection->find_test(parse_find_condition("{}"));
    REQUIRE(res->size() == 5);
}

TEST_CASE("collection_t delete_one") {
    auto collection = gen_collection();
    REQUIRE(collection->size_test() == 5);
    REQUIRE(collection->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 1);
    REQUIRE(collection->size_test() == 4);
    REQUIRE(collection->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 1);
    REQUIRE(collection->size_test() == 3);
    REQUIRE(collection->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 1);
    REQUIRE(collection->size_test() == 2);
    REQUIRE(collection->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 0);
    REQUIRE(collection->size_test() == 2);
}

TEST_CASE("collection_t delete_many") {
    auto collection = gen_collection();
    REQUIRE(collection->size_test() == 5);
    REQUIRE(collection->delete_many_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 3);
    REQUIRE(collection->size_test() == 2);
    REQUIRE(collection->delete_many_test(parse_find_condition("{\"type\": { \"$eq\": \"cat\"}}")).deleted_ids().size() == 2);
    REQUIRE(collection->size_test() == 0);
}

TEST_CASE("collection_t update_one set") {
    auto collection = gen_collection();
    REQUIRE(collection->get_test("id_1").get_string("name") == "Rex");

    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), false);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 1);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_string("name") == "Rex");

    result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                         document_t::from_json("{\"$set\": {\"name\": \"Adolf\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE_FALSE(collection->get_test("id_1").get_string("name") == "Rex");
    REQUIRE(collection->get_test("id_1").get_string("name") == "Adolf");

    result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_6\"}}"),
                                         document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), false);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 0);

    result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_6\"}}"),
                                         document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), true);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(!result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 1);
}

TEST_CASE("collection_t update_many set") {
    auto collection = gen_collection();
    REQUIRE(collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 1);

    auto result = collection->update_many_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}"),
                                               document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), false);
    REQUIRE(result.modified_ids().size() == 2);
    REQUIRE(result.nomodified_ids().size() == 1);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 3);

    result = collection->update_many_test(parse_find_condition("{\"type\": { \"$eq\": \"mouse\"}}"),
                                          document_t::from_json("{\"$set\": {\"name\": \"Mikki\", \"age\": 2}}"), false);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Mikki\"}}"))->size() == 0);

    result = collection->update_many_test(parse_find_condition("{\"type\": { \"$eq\": \"mouse\"}}"),
                                          document_t::from_json("{\"$set\": {\"name\": \"Mikki\", \"age\": 2}}"), true);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE_FALSE(result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Mikki\"}}"))->size() == 1);
}

TEST_CASE("collection_t update_one inc") {
    auto collection = gen_collection();
    REQUIRE(collection->get_test("id_1").get_ulong("age") == 6);

    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$inc\": {\"age\": 2}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_ulong("age") == 8);
}

TEST_CASE("collection_t update_one set new field") {
    auto collection = gen_collection();
    REQUIRE_FALSE(collection->get_test("id_1").is_exists("weight"));

    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"weight\": 8}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").is_exists("weight"));
    REQUIRE(collection->get_test("id_1").get_ulong("weight") == 8);
}

TEST_CASE("collection_t update_one set complex dict") {
    auto collection = gen_collection();
    REQUIRE_FALSE(collection->get_test("id_1").get_dict("sub_doc").get_dict("sub").get_string("name") == "Adolf");
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"sub_doc.sub.name\": \"Adolf\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_dict("sub_doc").get_dict("sub").get_string("name") == "Adolf");
}

TEST_CASE("collection_t update_one set complex array") {
    auto collection = gen_collection();
    REQUIRE_FALSE(collection->get_test("id_1").get_array("friends").get_array(2).get_as<std::string>(0) == "Adolf");
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"friends.2.0\": \"Adolf\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_array("friends").get_array(2).get_as<std::string>(0) == "Adolf");
}

TEST_CASE("collection_t update_one set complex dict with append new field") {
    auto collection = gen_collection();
    REQUIRE_FALSE(collection->get_test("id_1").is_exists("new_dict"));
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"new_dict.object.name\": \"NoName\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_dict("new_dict").get_dict("object").get_string("name") == "NoName");
}

TEST_CASE("collection_t update_one set complex array with append new field") {
    auto collection = gen_collection();
    REQUIRE_FALSE(collection->get_test("id_1").is_exists("new_array"));
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"new_array.1.5\": \"NoValue\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_array("new_array").get_array(0).get_as<std::string>(0) == "NoValue");
}

TEST_CASE("collection_t update_one set complex dict with append new subfield") {
    auto collection = gen_collection();
    REQUIRE_FALSE(collection->get_test("id_1").is_exists("new_dict"));
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_1\"}}"),
                                              document_t::from_json("{\"$set\": {\"sub_doc.sub2.name\": \"NoName\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_dict("sub_doc").get_dict("sub2").get_string("name") == "NoName");
}

TEST_CASE("collection_t update_one set complex dict with upsert") {
    auto collection = gen_collection();
    REQUIRE(collection->find_test(parse_find_condition("{\"new_dict.object.name\": {\"$eq\": \"NoName\"}}"))->size() == 0);
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_10\"}}"),
                                              document_t::from_json("{\"$set\": {\"new_dict.object.name\": \"NoName\"}}"), true);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE_FALSE(result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"new_dict.object.name\": {\"$eq\": \"NoName\"}}"))->size() == 1);
}

TEST_CASE("collection_t update_one set complex array with upsert") {
    auto collection = gen_collection();
    REQUIRE(collection->find_test(parse_find_condition("{\"new_array.0.0\": {\"$eq\": \"NoName\"}}"))->size() == 0);
    auto result = collection->update_one_test(parse_find_condition("{\"_id\": { \"$eq\": \"id_10\"}}"),
                                              document_t::from_json("{\"$set\": {\"new_array.0.0\": \"NoName\"}}"), true);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE_FALSE(result.upserted_id().empty());
    REQUIRE(collection->find_test(parse_find_condition("{\"new_array.0.0\": {\"$eq\": \"NoName\"}}"))->size() == 1);
}
