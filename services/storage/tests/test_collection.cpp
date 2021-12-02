#include <catch2/catch.hpp>
#include "database.hpp"
#include "collection.hpp"
#include "query.hpp"
#include "document/core/array.hpp"
#include "document/mutable/mutable_array.h"

using namespace services::storage;

document_t gen_sub_doc(const std::string &name, bool male) {
    document_t sub_doc;
    sub_doc.add_string("name", name);
    sub_doc.add_bool("male", male);
    return sub_doc;
}

document_t gen_doc(const std::string &id, const std::string &name, const std::string &type, ulong age, bool male,
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

TEST_CASE("collection_t search") {
    auto collection = gen_collection();
    REQUIRE(collection->search_test(eq("name", "Rex"))->size() == 1);
    REQUIRE(collection->search_test(gt("age", 2))->size() == 3);
    REQUIRE(collection->search_test(eq("type", "cat"))->size() == 2);
    REQUIRE(collection->search_test(eq("type", "dog") & lt("age", 5))->size() == 2);
    REQUIRE(collection->search_test(eq("type", "dog") & gte("age", 2) & lt("age", 5))->size() == 2);
    REQUIRE(collection->search_test(any("age", std::vector<int>{2,3,4}))->size() == 3);
    REQUIRE(collection->search_test(all("age", std::vector<int>{2,3}))->size() == 0);
    REQUIRE(collection->search_test(all("age", std::vector<int>{2}))->size() == 2);
    REQUIRE(collection->search_test(matches("name", "Ch.*"))->size() == 2);
    REQUIRE(collection->search_test(!eq("name", "Rex"))->size() == 4);
    REQUIRE(collection->search_test(!!eq("name", "Rex"))->size() == 1);
    REQUIRE(collection->search_test(!!!eq("name", "Rex"))->size() == 4);
}

TEST_CASE("collection_t find") {
    auto collection = gen_collection();
    auto res = collection->find_test(document_t::from_json("{\"name\": {\"$eq\": \"Rex\"}}"));
    REQUIRE(res->size() == 1);
    res = collection->find_test(document_t::from_json("{\"age\": {\"$gt\": 2, \"$lte\": 4}}"));
    REQUIRE(res->size() == 1);
    res = collection->find_test(document_t::from_json("{\"$and\": [{\"age\": {\"$gt\": 2}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(document_t::from_json("{\"$or\": [{\"name\": {\"$eq\": \"Rex\"}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
    REQUIRE(res->size() == 3);
    res = collection->find_test(document_t::from_json("{\"$not\": {\"type\": {\"$eq\": \"cat\"}}}"));
    REQUIRE(res->size() == 3);
    res = collection->find_test(document_t::from_json("{\"type\": {\"$in\": [\"cat\",\"dog\"]}}"));
    REQUIRE(res->size() == 5);
    res = collection->find_test(document_t::from_json("{\"name\": {\"$in\": [\"Rex\",\"Lucy\",\"Tank\"]}}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(document_t::from_json("{\"name\": {\"$all\": [\"Rex\",\"Lucy\"]}}"));
    REQUIRE(res->size() == 0);
    res = collection->find_test(document_t::from_json("{\"name\": {\"$all\": [\"Rex\",\"Rex\"]}}"));
    REQUIRE(res->size() == 1);
    res = collection->find_test(document_t::from_json("{\"name\": {\"$regex\": \"Ch.*\"}}"));
    REQUIRE(res->size() == 2);
    res = collection->find_test(document_t::from_json("{}"));
    REQUIRE(res->size() == 5);
}

TEST_CASE("collection_t delete_one") {
    auto collection = gen_collection();
    REQUIRE(collection->size_test() == 5);
    REQUIRE(collection->delete_one_test(eq("type", "dog")).deleted_ids().size() == 1);
    REQUIRE(collection->size_test() == 4);
    REQUIRE(collection->delete_one_test(eq("type", "dog")).deleted_ids().size() == 1);
    REQUIRE(collection->size_test() == 3);
    REQUIRE(collection->delete_one_test(eq("type", "dog")).deleted_ids().size() == 1);
    REQUIRE(collection->size_test() == 2);
    REQUIRE(collection->delete_one_test(eq("type", "dog")).deleted_ids().size() == 0);
    REQUIRE(collection->size_test() == 2);
}

TEST_CASE("collection_t delete_many") {
    auto collection = gen_collection();
    REQUIRE(collection->size_test() == 5);
    REQUIRE(collection->delete_many_test(eq("type", "dog")).deleted_ids().size() == 3);
    REQUIRE(collection->size_test() == 2);
    REQUIRE(collection->delete_many_test(eq("type", "cat")).deleted_ids().size() == 2);
    REQUIRE(collection->size_test() == 0);
}

TEST_CASE("collection_t update_one") {
    auto collection = gen_collection();
    REQUIRE(collection->get_test("id_1").get_string("name") == "Rex");

    auto result = collection->update_one_test(eq("_id", "id_1"), document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), false);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 1);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->get_test("id_1").get_string("name") == "Rex");

    result = collection->update_one_test(eq("_id", "id_1"), document_t::from_json("{\"$set\": {\"name\": \"Adolf\"}}"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE_FALSE(collection->get_test("id_1").get_string("name") == "Rex");
    REQUIRE(collection->get_test("id_1").get_string("name") == "Adolf");

    result = collection->update_one_test(eq("_id", "id_6"), document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), false);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(result.upserted_id().empty());
    REQUIRE(collection->find_test(document_t::from_json("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 0);

    result = collection->update_one_test(eq("_id", "id_6"), document_t::from_json("{\"$set\": {\"name\": \"Rex\"}}"), true);
    REQUIRE(result.modified_ids().size() == 0);
    REQUIRE(result.nomodified_ids().size() == 0);
    REQUIRE(!result.upserted_id().empty());
    REQUIRE(collection->find_test(document_t::from_json("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 1);
}
