#pragma once

#include <integration/python/spaces.hpp>
#include <benchmark/benchmark.h>
#include <components/document/document.hpp>
#include <components/document/core/array.hpp>
#include <components/document/mutable/mutable_dict.h>
#include <components/tests/generaty.hpp>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name = "TestCollection";
static constexpr int size_collection = 10000;


inline configuration::config create_null_config() {
    auto config = configuration::config::default_config();
    config.log.level = log_t::level::off;
    config.disk.on = false;
    config.wal.sync_to_disk = false;
    return config;
}

class unique_spaces final : public duck_charmer::base_spaces {
public:
    static unique_spaces &get() {
        static unique_spaces spaces_;
        return spaces_;
    }

private:
    unique_spaces()
        : duck_charmer::base_spaces(create_null_config())
    {}
};


void init_collection() {
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = duck_charmer::session_id_t();
    dispatcher->create_database(session, database_name);
    dispatcher->create_collection(session, database_name, collection_name);
    std::list<document_ptr> docs;
    for (int i = 1; i <= size_collection; ++i) {
        docs.push_back(gen_doc(i));
    }
    dispatcher->insert_many(session, database_name, collection_name, docs);
}

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
    auto array = array_t::new_array();
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
