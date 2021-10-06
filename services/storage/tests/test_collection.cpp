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
//    print_search("===> Search (name == Rex)", collection->search(query_t<std::string>("name") == "Rex"));
//    print_search("===> Search (age > 3)", collection->search(query_t<long>("age") > 3));
//    print_search("===> Search (type == dog)", collection->search(query_t<std::string>("type") == "dog"));
//    print_search("===> Search (male)", collection->search(query_t<bool>("male") == true));
//    print_search("===> Search (type any (dog, cat))", collection->search(query_t<std::string>("type").any(std::vector<std::string>{"dog", "cat"})));
}
