#include "benchmark_collection.hpp"
#include <components/protocol/base.hpp>
#include <components/tests/generaty.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>

//BENCHMARK_MAIN();

template<class T>
document::retained_t<mutable_dict_t> make_dict(const std::string& field, const std::string& key, T value) {
    auto key_value = mutable_dict_t::new_dict();
    key_value->set(key, value);
    auto cond = mutable_dict_t::new_dict();
    cond->set(field, key_value);
    return cond;
}

template<class T>
document_ptr make_condition(const std::string& field, const std::string& key, T value) {
    auto dict = make_dict(field, key, value);
    return make_document(dict);
}

inline document::retained_t<mutable_dict_t> make_dict(const std::string& aggregate, const std::list<document::retained_t<mutable_dict_t>> &sub_dict) {
    auto dict = mutable_dict_t::new_dict();
    auto array = mutable_array_t::new_array();
    for (const auto& sub_cond : sub_dict) {
        array->append(sub_cond);
    }
    dict->set(aggregate, array);
    return dict;
}

inline document_ptr make_condition(const std::string& aggregate, const std::list<document::retained_t<mutable_dict_t>> &sub_conditions) {
    auto dict = make_dict(aggregate, sub_conditions);
    return make_document(dict);
}

int main() {
    static const database_name_t database_name = "TestDatabase";
    static const collection_name_t collection_name = "TestCollection";

    test_spaces space;
    auto* dispatcher = space.dispatcher();

    {
        auto session = duck_charmer::session_id_t();
        dispatcher->create_database(session, database_name);
    }
    {
        auto session = duck_charmer::session_id_t();
        dispatcher->create_collection(session, database_name, collection_name);
    }
    {
        auto session = duck_charmer::session_id_t();
        dispatcher->size(session, database_name, collection_name);
    }

    for (int num = 0; num < 50; ++num) {
        {
            auto doc = gen_doc(num);
            auto session = duck_charmer::session_id_t();
            dispatcher->insert_one(session, database_name, collection_name, doc);
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->size(session, database_name, collection_name);
        }
    }
    {
        auto session = duck_charmer::session_id_t();
        dispatcher->size(session, database_name, collection_name);
    }

    std::list<components::document::document_ptr> documents;
    for (int num = 50; num < 100; ++num) {
        documents.push_back(gen_doc(num));
    }
    {
        auto session = duck_charmer::session_id_t();
        dispatcher->insert_many(session, database_name, collection_name, documents);
    }
    {
        auto session = duck_charmer::session_id_t();
        dispatcher->size(session, database_name, collection_name);
    }

    for (int num = 0; num < 100; ++num) {
        {
            auto doc = gen_doc(num);
            auto session = duck_charmer::session_id_t();
            dispatcher->insert_one(session, database_name, collection_name, doc);
        }
        {
            auto session = duck_charmer::session_id_t();
            dispatcher->size(session, database_name, collection_name);
        }
    }
    {
        auto session = duck_charmer::session_id_t();
        dispatcher->size(session, database_name, collection_name);
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_document());
        c->size();
        delete c;
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_condition("count", "$gt", 90));
        c->size();
        delete c;
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_condition("countStr", "$regex", std::string("9$")));
        c->size();
        delete c;
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_condition("$or", {make_dict("count", "$gt", 90), make_dict("countStr", "$regex", std::string("9$"))}));
        c->size();
        delete c;
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_condition("$and", {make_dict("$or", {make_dict("count", "$gt", 90), make_dict("countStr", "$regex", std::string("9$"))}), make_dict("count", "$lte", 30)}));
        c->size();
        delete c;
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find(session, database_name, collection_name, make_document());
        c->size();
        int count = 0;
        while (c->has_next()) {
            c->next();
            ++count;
        }
        count;
        delete c;
    }

    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find_one(session, database_name, collection_name, make_condition("_id", "$eq", gen_id(1)));
        c->get_long("count");
    }
    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find_one(session, database_name, collection_name, make_condition("count", "$eq", 10));
        c->get_long("count");
    }
    {
        auto session = duck_charmer::session_id_t();
        auto c = dispatcher->find_one(session, database_name, collection_name, make_condition("$and", {make_dict("count", "$gt", 90), make_dict("countStr", "$regex", std::string("9$"))}));
        c->get_long("count");
    }
}
