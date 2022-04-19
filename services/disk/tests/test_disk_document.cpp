#include <catch2/catch.hpp>
#include <components/document/document_view.hpp>
#include <disk/disk.hpp>
#include <components/tests/generaty.hpp>

const std::string file_db = "/tmp/documents.rdb";
const std::string database_name = "test_database";
const std::string collection_name = "test_collection";

using namespace services::disk;

TEST_CASE("sync documents from disk") {
    SECTION("save document into disk") {
        remove((file_db + "/metadata").data());
        disk_t disk(file_db);
        for (int num = 1; num <= 100; ++num) {
            disk.save_document(database_name, collection_name, document_id_t(gen_id(num)), gen_doc(num));
        }
    }

    SECTION("load document from disk") {
        disk_t disk(file_db);
        for (int num = 1; num <= 100; ++num) {
            auto doc = disk.load_document(database_name, collection_name, document_id_t(gen_id(num)));
            REQUIRE(doc != nullptr);
            components::document::document_view_t doc_view(doc->structure, &doc->data);
            REQUIRE(doc_view.get_string("_id") == gen_id(num));
            REQUIRE(doc_view.get_long("count") == num);
            REQUIRE(doc_view.get_string("countStr") == std::to_string(num));
            REQUIRE(doc_view.get_double("countDouble") == Approx(float(num) + 0.1));
            REQUIRE(doc_view.get_bool("countBool") == (num % 2 != 0));
            REQUIRE(doc_view.get_array("countArray").count() == 5);
            REQUIRE(doc_view.get_dict("countDict").count() == 4);
        }
    }

    SECTION("delete document from disk") {
        disk_t disk(file_db);
        for (int num = 1; num <= 100; num += 2) {
            disk.remove_document(database_name, collection_name, document_id_t(gen_id(num)));
        }
        for (int num = 1; num <= 100; ++num) {
            auto doc = disk.load_document(database_name, collection_name, document_id_t(gen_id(num)));
            if (num % 2 == 0) {
                REQUIRE(doc != nullptr);
                components::document::document_view_t doc_view(doc->structure, &doc->data);
                REQUIRE(doc_view.get_string("_id") == gen_id(num));
                REQUIRE(doc_view.get_long("count") == num);
                REQUIRE(doc_view.get_string("countStr") == std::to_string(num));
                REQUIRE(doc_view.get_double("countDouble") == Approx(float(num) + 0.1));
                REQUIRE(doc_view.get_bool("countBool") == (num % 2 != 0));
                REQUIRE(doc_view.get_array("countArray").count() == 5);
                REQUIRE(doc_view.get_dict("countDict").count() == 4);
            } else {
                REQUIRE(doc == nullptr);
            }
        }
    }

    SECTION("load list documents from disk") {
        disk_t disk(file_db);
        auto id_documents = disk.load_list_documents(database_name, collection_name);
        REQUIRE(id_documents.size() == 50);
        for (const auto &id : id_documents) {
            auto doc = disk.load_document(id);
            REQUIRE(doc != nullptr);
        }
    }
}