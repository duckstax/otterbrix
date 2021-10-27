#include <catch2/catch.hpp>
#include "database.hpp"
#include "collection.hpp"
#include "query.hpp"

using namespace services::storage;

document_t gen_doc(const std::string &id, const std::string &name, const std::string &type, ulong age, bool male) {
    document_t doc;
    doc.add_string("_id", id);
    doc.add_string("name", name);
    doc.add_string("type", type);
    doc.add_ulong("age", age);
    doc.add_bool("male", male);
    return doc;
}

collection_ptr gen_collection() {
    static auto log = initialization_logger("duck_charmer", "/tmp/docker_logs/");
    log.set_level(log_t::level::trace);
    auto collection = goblin_engineer::make_manager_service<collection_t>(nullptr, log);

    collection->insert_test(gen_doc("1", "Rex", "dog", 6, true));
    collection->insert_test(gen_doc("2", "Lucy", "dog", 2, false));
    collection->insert_test(gen_doc("3", "Chevi", "cat", 3, false));
    collection->insert_test(gen_doc("4", "Charlie", "dog", 2, true));
    collection->insert_test(gen_doc("5", "Isha", "cat", 6, false));

    return collection;
}

void print_doc(document_t *doc) {
    std::cout << "Doc " << doc->get_as<std::string>("id") << " {\n"
              << "    name: " << doc->get_as<std::string>("name") << ",\n"
              << "    type: " << doc->get_as<std::string>("type") << ",\n"
              << "    age: "  << doc->get_as<ulong>("age") << ",\n"
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
    std::cout << "STRUCTURE:\n" << collection->get_structure_test() << std::endl;
    std::cout << "INDEX:\n" << collection->get_index_test() << std::endl;
    std::cout << "DATA:\n" << collection->get_data_test() << std::endl;

//    REQUIRE(collection->search_test(eq("name", "Rex")).size() == 1);
//    REQUIRE(collection->search_test(gt("age", 2)).size() == 3);
//    REQUIRE(collection->search_test(eq("type", "cat")).size() == 2);
//    REQUIRE(collection->search_test(eq("type", "dog") & lt("age", 5)).size() == 2);
//    REQUIRE(collection->search_test(eq("type", "dog") & gte("age", 2) & lt("age", 5)).size() == 2);
//    REQUIRE(collection->search_test(any("age", std::vector<int>{2,3,4})).size() == 3);
//    REQUIRE(collection->search_test(all("age", std::vector<int>{2,3})).size() == 0);
//    REQUIRE(collection->search_test(all("age", std::vector<int>{2})).size() == 2);
//    REQUIRE(collection->search_test(matches("name", "Ch.*")).size() == 2);
//    REQUIRE(collection->search_test(!eq("name", "Rex")).size() == 4);
//    REQUIRE(collection->search_test(!!eq("name", "Rex")).size() == 1);
//    REQUIRE(collection->search_test(!!!eq("name", "Rex")).size() == 4);
//    delete collection.detach(); //todo delete after repair ref count
}

//TEST_CASE("collection_t find") {
//    auto collection = gen_collection();
//    auto res = collection->find_test(document_t::json_t::parse("{\"name\": {\"$eq\": \"Rex\"}}"));
//    REQUIRE(res.size() == 1);
//    res = collection->find_test(document_t::json_t::parse("{\"age\": {\"$gt\": 2, \"$lte\": 4}}"));
//    REQUIRE(res.size() == 1);
//    res = collection->find_test(document_t::json_t::parse("{\"$and\": [{\"age\": {\"$gt\": 2}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
//    REQUIRE(res.size() == 2);
//    res = collection->find_test(document_t::json_t::parse("{\"$or\": [{\"name\": {\"$eq\": \"Rex\"}}, {\"type\": {\"$eq\": \"cat\"}}]}"));
//    REQUIRE(res.size() == 3);
//    res = collection->find_test(document_t::json_t::parse("{\"$not\": {\"type\": {\"$eq\": \"cat\"}}}"));
//    REQUIRE(res.size() == 3);
//    res = collection->find_test(document_t::json_t::parse("{\"type\": {\"$in\": [\"cat\",\"dog\"]}}"));
//    REQUIRE(res.size() == 5);
//    res = collection->find_test(document_t::json_t::parse("{\"name\": {\"$in\": [\"Rex\",\"Lucy\",\"Tank\"]}}"));
//    REQUIRE(res.size() == 2);
//    res = collection->find_test(document_t::json_t::parse("{\"name\": {\"$all\": [\"Rex\",\"Lucy\"]}}"));
//    REQUIRE(res.size() == 0);
//    res = collection->find_test(document_t::json_t::parse("{\"name\": {\"$all\": [\"Rex\",\"Rex\"]}}"));
//    REQUIRE(res.size() == 1);
//    res = collection->find_test(document_t::json_t::parse("{\"name\": {\"$regex\": \"Ch.*\"}}"));
//    REQUIRE(res.size() == 2);
//    delete collection.detach(); //todo delete after repair ref count
//}
