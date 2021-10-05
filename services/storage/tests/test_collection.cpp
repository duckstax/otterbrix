#include <catch2/catch.hpp>
#include "collection.hpp"
#include "database.hpp"

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

TEST_CASE("collection_t insert") {

    auto collection = gen_collection();

}
