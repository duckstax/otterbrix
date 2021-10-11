#include <catch2/catch.hpp>
#include "database.hpp"
#include "collection.hpp"
#include "query.hpp"

using namespace services::storage;

collection_ptr gen_collection() {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto collection = goblin_engineer::make_manager_service<collection_t>(nullptr, log);

    document_t doc1;
    doc1.add("_id", std::string("1"));
    doc1.add("name", std::string("Rex"));
    doc1.add("type", std::string("dog"));
    doc1.add("age", 6l);
    doc1.add("male", true);
    collection->dummy_insert(std::move(doc1));

    document_t doc2;
    doc2.add("_id", std::string("2"));
    doc2.add("name", std::string("Lucy"));
    doc2.add("type", std::string("dog"));
    doc2.add("age", 2l);
    doc2.add("male", false);
    collection->dummy_insert(std::move(doc2));

    document_t doc3;
    doc3.add("_id", std::string("3"));
    doc3.add("name", std::string("Chevi"));
    doc3.add("type", std::string("cat"));
    doc3.add("age", 3l);
    doc3.add("male", false);
    collection->dummy_insert(std::move(doc3));

    document_t doc4;
    doc4.add("_id", std::string("4"));
    doc4.add("name", std::string("Charlie"));
    doc4.add("type", std::string("dog"));
    doc4.add("age", 2l);
    doc4.add("male", true);
    collection->dummy_insert(std::move(doc4));

    document_t doc5;
    doc5.add("_id", std::string("5"));
    doc5.add("name", std::string("Isha"));
    doc5.add("type", std::string("cat"));
    doc5.add("age", 6l);
    doc5.add("male", false);
    collection->dummy_insert(std::move(doc5));

    return collection;
}

void print_doc(document_t *doc) {
    std::cout << "Doc " << doc->get_as<std::string>("_id") << " {\n"
              << "    name: " << doc->get_as<std::string>("name") << ",\n"
              << "    type: " << doc->get_as<std::string>("type") << ",\n"
              << "    age: " << doc->get_as<long>("age") << ",\n"
              << "    male: " << doc->get_as<bool>("male") << ",\n"
              << "}" << std::endl;
}

void print_search(const std::string &search, const std::list<document_t*> docs) {
    std::cout << search << std::endl;
    for (auto doc : docs) {
        print_doc(doc);
    }
}

TEST_CASE("collection_t search") {
    auto collection = gen_collection();
    REQUIRE(collection->search(eq("name", "Rex")).size() == 1);
    REQUIRE(collection->search(gt("age", 2)).size() == 3);
    REQUIRE(collection->search(eq("type", "cat")).size() == 2);
    REQUIRE(collection->search(eq("type", "dog") & lt("age", 5)).size() == 2);
    REQUIRE(collection->search(eq("type", "dog") & gte("age", 2) & lt("age", 5)).size() == 2);
    REQUIRE(collection->search(any("age", std::vector<int>{2,3,4})).size() == 3);
    REQUIRE(collection->search(all("age", std::vector<int>{2,3})).size() == 0);
    REQUIRE(collection->search(all("age", std::vector<int>{2})).size() == 2);
    REQUIRE(collection->search(matches("name", "Ch*")).size() == 2);
    REQUIRE(collection->search(!eq("name", "Rex")).size() == 4);
    REQUIRE(collection->search(!!eq("name", "Rex")).size() == 1);
    REQUIRE(collection->search(!!!eq("name", "Rex")).size() == 4);
}

TEST_CASE("collection_t find") {
    auto collection = gen_collection();
    auto res = collection->find(document_t::json_t::parse("{\"name\": {\"$eq\": \"Rex\"}}"));
    REQUIRE(res.size() == 1);
    res = collection->find(document_t::json_t::parse("{\"age\": {\"$gt\": 2, \"$lte\": 4}}"));
    REQUIRE(res.size() == 1);
    res = collection->find(document_t::json_t::parse("{\"$and\": [{\"age\": {\"$gt\": 2}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
    REQUIRE(res.size() == 2);
    res = collection->find(document_t::json_t::parse("{\"$or\": [{\"name\": {\"$eq\": \"Rex\"}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
    REQUIRE(res.size() == 3);
    res = collection->find(document_t::json_t::parse("{\"$not\": {\"type\": {\"$eq\": \"cat\"}}}"));
    REQUIRE(res.size() == 3);
    res = collection->find(document_t::json_t::parse("{\"type\": {\"$in\": [\"cat\",\"dog\"]}}"));
    REQUIRE(res.size() == 5);
    res = collection->find(document_t::json_t::parse("{\"name\": {\"$in\": [\"Rex\",\"Lucy\",\"Tank\"]}}"));
    REQUIRE(res.size() == 2);
    res = collection->find(document_t::json_t::parse("{\"name\": {\"$all\": [\"Rex\",\"Lucy\"]}}"));
    REQUIRE(res.size() == 0);
    res = collection->find(document_t::json_t::parse("{\"name\": {\"$all\": [\"Rex\",\"Rex\"]}}"));
    REQUIRE(res.size() == 1);
//    res = collection->find(document_t::json_t::parse("{\"name\": {\"$regex\": \"Ch*\"}}"));
//    REQUIRE(res.size() == 2);
}
