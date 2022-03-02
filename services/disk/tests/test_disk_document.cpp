#include <catch2/catch.hpp>
#include <components/btree_storage/btree_storage.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include <components/document/document_view.hpp>
#include <disk/disk.hpp>

using namespace components::btree;

const std::string file_db = "/tmp/documents.rdb";
const std::string database_name = "test_database";
const std::string collection_name = "test_collection";

document::impl::array_t *gen_array(int num) {
    auto array = document::impl::mutable_array_t::new_array().detach();
    for (int i = 0; i < 5; ++i) {
        array->append(num + i);
    }
    return array;
}

document::impl::dict_t *gen_dict(int num) {
    auto dict = document::impl::mutable_dict_t::new_dict().detach();
    dict->set("odd", num % 2 != 0);
    dict->set("even", num % 2 == 0);
    dict->set("three", num % 3 == 0);
    dict->set("five", num % 5 == 0);
    return reinterpret_cast<document::impl::dict_t *>(dict);
}

input_document_t gen_input_doc(int num) {
    input_document_t doc;
    doc.add_string("_id", std::to_string(num));
    doc.add_long("count", num);
    doc.add_string("countStr", std::to_string(num));
    doc.add_double("countDouble", float(num) + 0.1);
    doc.add_bool("countBool", num % 2 != 0);
    doc.add_array("countArray", gen_array(num));
    doc.add_dict("countDict", gen_dict(num));
    auto array = document::impl::mutable_array_t::new_array().detach();
    for (int i = 0; i < 5; ++i) {
        array->append(gen_array(num + i));
    }
    doc.add_array("nestedArray", array);
    array = document::impl::mutable_array_t::new_array().detach();
    for (int i = 0; i < 5; ++i) {
        auto dict = document::impl::mutable_dict_t::new_dict().detach();
        dict->set("number", num + i);
        array->append(dict);
    }
    doc.add_array("dictArray", array);
    auto dict = document::impl::mutable_dict_t::new_dict().detach();
    for (int i = 0; i < 5; ++i) {
        dict->set(std::to_string(num + i), gen_dict(num + i));
    }
    doc.add_dict("mixedDict", dict);
    return doc;
}

document_unique_ptr gen_doc(int num) {
    return make_document(gen_input_doc(num));
}


using namespace services::disk;

TEST_CASE("sync documents from disk") {
    SECTION("save document into disk") {
        disk_t disk(file_db);
        for (int num = 1; num <= 100; ++num) {
            disk.save_document(database_name, collection_name, document_id_t(std::to_string(num)), gen_doc(num));
        }
    }

    SECTION("load document from disk") {
        disk_t disk(file_db);
        for (int num = 1; num <= 100; ++num) {
            auto doc = disk.load_document(database_name, collection_name, document_id_t(std::to_string(num)));
            REQUIRE(doc != nullptr);
            components::document::document_view_t doc_view(doc->structure, &doc->data);
            REQUIRE(doc_view.get_string("_id") == std::to_string(num));
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
            disk.remove_document(database_name, collection_name, document_id_t(std::to_string(num)));
        }
        for (int num = 1; num <= 100; ++num) {
            auto doc = disk.load_document(database_name, collection_name, document_id_t(std::to_string(num)));
            if (num % 2 == 0) {
                REQUIRE(doc != nullptr);
                components::document::document_view_t doc_view(doc->structure, &doc->data);
                REQUIRE(doc_view.get_string("_id") == std::to_string(num));
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