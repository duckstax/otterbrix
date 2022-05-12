#include <services/collection/collection.hpp>
#include <services/database/database.hpp>
#include <document/mutable/mutable_dict.h>
#include <document/mutable/mutable_array.h>
#include <parser/parser.hpp>
#include <catch2/catch.hpp>

using namespace services::collection;
using namespace services::database;

document::retained_t<document::impl::mutable_dict_t> gen_sub_doc(const std::string& name, bool male) {
    auto dict = ::document::impl::mutable_dict_t::new_dict();
    dict->set("name", name);
    dict->set("male", male);
    return dict;
}

document_ptr gen_doc(const std::string& id, const std::string& name, const std::string& type, ulong age, bool male,
                     std::initializer_list<std::string> friends, const document::retained_t<document::impl::mutable_dict_t> &sub_doc) {
    auto dict = document::impl::mutable_dict_t::new_dict();
    dict->set("_id", id);
    dict->set("name", name);
    dict->set("type", type);
    dict->set("age", age);
    dict->set("male", male);
    auto array = ::document::impl::mutable_array_t::new_array();
    for (const auto& value : friends) {
        array->append(value);
    }
    array->append(array->copy());
    dict->set("friends", array);
    sub_doc->set("sub", gen_sub_doc(name, male));
    dict->set("sub_doc", sub_doc);

    return components::document::make_document(dict);
}

struct context_t final  {
    using collection_ptr = actor_zeta::intrusive_ptr<collection_t>;

    collection_t* operator->() const noexcept {
        return collection_.get();
    }

    collection_t& operator*() const noexcept {
        return *(collection_);
    }

    actor_zeta::detail::pmr::memory_resource *resource;
    std::unique_ptr<manager_database_t> manager_database_;
    std::unique_ptr<database_t> database_;
    std::unique_ptr<collection_t> collection_;
};


using context_ptr = std::unique_ptr<context_t>;

context_ptr make_context(log_t& log) {
    auto* context = new context_t;
    context->resource = actor_zeta::detail::pmr::get_default_resource();
    context->manager_database_ = actor_zeta::spawn_supervisor<manager_database_t>(context->resource,log,1,1000);
    context->database_ = actor_zeta::spawn_supervisor<database_t>(context->manager_database_.get(),"TestDataBase",log,1,1000);

    auto allocate_byte = sizeof(collection_t);
    auto allocate_byte_alignof = alignof(collection_t);
    void* buffer = context->resource->allocate(allocate_byte, allocate_byte_alignof);
    auto* collection = new (buffer) collection_t(context->database_.get(), "TestCollection", log, actor_zeta::address_t::empty_address());
    context->collection_.reset(collection);
};

collection_t* d(context_ptr&ptr){
    return ptr->collection_.get();
}

context_ptr gen_collection() {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);

    auto collection = make_context(log);

    d(collection)->insert_test(gen_doc("12345678123456789a00ff01", "Rex", "dog", 6, true, {"Lucy", "Charlie"}, gen_sub_doc("Lucy", true)));
    d(collection)->insert_test(gen_doc("12345678123456789a00ff02", "Lucy", "dog", 2, false, {"Rex", "Charlie"}, gen_sub_doc("Rex", true)));
    d(collection)->insert_test(gen_doc("12345678123456789a00ff03", "Chevi", "cat", 3, false, {"Isha"}, gen_sub_doc("Isha", true)));
    d(collection)->insert_test(gen_doc("12345678123456789a00ff04", "Charlie", "dog", 2, true, {"Rex", "Lucy"}, gen_sub_doc("Lucy", false)));
    d(collection)->insert_test(gen_doc("12345678123456789a00ff05", "Isha", "cat", 6, false, {"Chevi"}, gen_sub_doc("Chevi", true)));

    return collection;
}

find_condition_ptr parse_find_condition(const std::string& cond) {
    return components::parser::parse_find_condition(components::document::document_from_json(cond));
}

TEST_CASE("collection_t get") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->size_test() == 5);

    auto doc1 = d(collection)->get_test("12345678123456789a00ff01");
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
    REQUIRE(doc1_friends.is_array());
    REQUIRE(doc1_friends.count() == 3);
    REQUIRE(doc1_friends.get_as<std::string>(1) == "Charlie");

    auto doc1_sub = doc1.get_dict("sub_doc");
    REQUIRE(doc1_sub.is_dict());
    REQUIRE(doc1_sub.count() == 3);
    REQUIRE(doc1_sub.get_as<std::string>("name") == "Lucy");

    auto doc6 = d(collection)->get_test("12345678123456789a00ff06");
    REQUIRE_FALSE(doc6.is_valid());
}

TEST_CASE("collection_t find") {
    auto collection = gen_collection();
    auto res = d(collection)->find_test(parse_find_condition(R"({"name": {"$eq": "Rex"}})"));
    REQUIRE(res->size() == 1);
    res = d(collection)->find_test(parse_find_condition(R"({"age": {"$gt": 2, "$lte": 4}})"));
    REQUIRE(res->size() == 1);
    res = d(collection)->find_test(parse_find_condition(R"({"$and": [{"age": {"$gt": 2}}, {"type": {"$eq": "cat"}}]})"));
    REQUIRE(res->size() == 2);
    res = d(collection)->find_test(parse_find_condition(R"({"$or": [{"name": {"$eq": "Rex"}}, {"type": {"$eq": "cat"}}]})"));
    REQUIRE(res->size() == 3);
    res = d(collection)->find_test(parse_find_condition(R"({"$not": {"type": {"$eq": "cat"}}})"));
    REQUIRE(res->size() == 3);
    res = d(collection)->find_test(parse_find_condition(R"({"type": {"$in": ["cat","dog"]}})"));
    REQUIRE(res->size() == 5);
    res = d(collection)->find_test(parse_find_condition(R"({"name": {"$in": ["Rex","Lucy","Tank"]}})"));
    REQUIRE(res->size() == 2);
    res = d(collection)->find_test(parse_find_condition(R"({"name": {"$all": ["Rex","Lucy"]}})"));
    REQUIRE(res->size() == 2);
    res = d(collection)->find_test(parse_find_condition(R"({"name": {"$regex": "^Ch"}})"));
    REQUIRE(res->size() == 2);
    res = d(collection)->find_test(parse_find_condition("{}"));
    REQUIRE(res->size() == 5);
    res = d(collection)->find_test(parse_find_condition("{}"));
    REQUIRE(res->size() == 5);
}

TEST_CASE("collection_t delete_one") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->size_test() == 5);
    REQUIRE(d(collection)->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 1);
    REQUIRE(d(collection)->size_test() == 4);
    REQUIRE(d(collection)->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 1);
    REQUIRE(d(collection)->size_test() == 3);
    REQUIRE(d(collection)->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 1);
    REQUIRE(d(collection)->size_test() == 2);
    REQUIRE(d(collection)->delete_one_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().empty());
    REQUIRE(d(collection)->size_test() == 2);
}

TEST_CASE("collection_t delete_many") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->size_test() == 5);
    REQUIRE(d(collection)->delete_many_test(parse_find_condition("{\"type\": { \"$eq\": \"dog\"}}")).deleted_ids().size() == 3);
    REQUIRE(d(collection)->size_test() == 2);
    REQUIRE(d(collection)->delete_many_test(parse_find_condition("{\"type\": { \"$eq\": \"cat\"}}")).deleted_ids().size() == 2);
    REQUIRE(d(collection)->size_test() == 0);
}

TEST_CASE("collection_t update_one set") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_string("name") == "Rex");

    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Rex"}})"), false);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().size() == 1);
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_string("name") == "Rex");

    result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                         components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Adolf"}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").get_string("name") == "Rex");
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_string("name") == "Adolf");

    result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff06"}})"),
                                         components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Rex"}})"), false);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->empty());

    result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff06"}})"),
                                         components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Rex"}})"), true);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(!result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 1);
}

TEST_CASE("collection_t update_many set") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 1);

    auto result = d(collection)->update_many_test(parse_find_condition(R"({"type": { "$eq": "dog"}})"),
                                               components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Rex"}})"), false);
    REQUIRE(result.modified_ids().size() == 2);
    REQUIRE(result.nomodified_ids().size() == 1);
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Rex\"}}"))->size() == 3);

    result = d(collection)->update_many_test(parse_find_condition(R"({"type": { "$eq": "mouse"}})"),
                                          components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Mikki", "age": 2}})"), false);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Mikki\"}}"))->empty());

    result = d(collection)->update_many_test(parse_find_condition(R"({"type": { "$eq": "mouse"}})"),
                                          components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"name": "Mikki", "age": 2}})"), true);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE_FALSE(result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"name\": {\"$eq\": \"Mikki\"}}"))->size() == 1);
}

TEST_CASE("collection_t update_one inc") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_ulong("age") == 6);

    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$inc": {"age": 2}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_ulong("age") == 8);
}

TEST_CASE("collection_t update_one set new field") {
    auto collection = gen_collection();
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").is_exists("weight"));

    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"weight": 8}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").is_exists("weight"));
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_ulong("weight") == 8);
}

TEST_CASE("collection_t update_one set complex dict") {
    auto collection = gen_collection();
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").get_dict("sub_doc").get_dict("sub").get_string("name") == "Adolf");
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"sub_doc.sub.name": "Adolf"}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_dict("sub_doc").get_dict("sub").get_string("name") == "Adolf");
}

TEST_CASE("collection_t update_one set complex array") {
    auto collection = gen_collection();
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").get_array("friends").get_array(2).get_as<std::string>(0) == "Adolf");
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"friends.2.0": "Adolf"}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_array("friends").get_array(2).get_as<std::string>(0) == "Adolf");
}

TEST_CASE("collection_t update_one set complex dict with append new field") {
    auto collection = gen_collection();
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").is_exists("new_dict"));
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"new_dict.object.name": "NoName"}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_dict("new_dict").get_dict("object").get_string("name") == "NoName");
}

TEST_CASE("collection_t update_one set complex array with append new field") {
    auto collection = gen_collection();
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").is_exists("new_array"));
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"new_array.1.5": "NoValue"}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_array("new_array").get_array(0).get_as<std::string>(0) == "NoValue");
}

TEST_CASE("collection_t update_one set complex dict with append new subfield") {
    auto collection = gen_collection();
    REQUIRE_FALSE(d(collection)->get_test("12345678123456789a00ff01").is_exists("new_dict"));
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff01"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff06", "$set": {"sub_doc.sub2.name": "NoName"}})"), false);
    REQUIRE(result.modified_ids().size() == 1);
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE(result.upserted_id().is_null());
    REQUIRE(d(collection)->get_test("12345678123456789a00ff01").get_dict("sub_doc").get_dict("sub2").get_string("name") == "NoName");
}

TEST_CASE("collection_t update_one set complex dict with upsert") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"new_dict.object.name\": {\"$eq\": \"NoName\"}}"))->empty());
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff10"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff10", "$set": {"new_dict.object.name": "NoName"}})"), true);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE_FALSE(result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"new_dict.object.name\": {\"$eq\": \"NoName\"}}"))->size() == 1);
}

TEST_CASE("collection_t update_one set complex array with upsert") {
    auto collection = gen_collection();
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"new_array.0.0\": {\"$eq\": \"NoName\"}}"))->empty());
    auto result = d(collection)->update_one_test(parse_find_condition(R"({"_id": { "$eq": "12345678123456789a00ff10"}})"),
                                              components::document::document_from_json(R"({"_id": "12345678123456789a00ff10", "$set": {"new_array.0.0": "NoName"}})"), true);
    REQUIRE(result.modified_ids().empty());
    REQUIRE(result.nomodified_ids().empty());
    REQUIRE_FALSE(result.upserted_id().is_null());
    REQUIRE(d(collection)->find_test(parse_find_condition("{\"new_array.0.0\": {\"$eq\": \"NoName\"}}"))->size() == 1);
}
