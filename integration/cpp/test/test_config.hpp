#pragma once

#include <components/document/document.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>
#include <components/tests/generaty.hpp>
#include <integration/cpp/base_spaces.hpp>

inline configuration::config test_create_config(const std::filesystem::path &path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.disk.path = path;
    config.wal.path = path;
    return config;
}

inline void test_clear_directory(const configuration::config &config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

class test_spaces final : public duck_charmer::base_spaces {
public:
    test_spaces(const configuration::config &config)
        : duck_charmer::base_spaces(config)
    {}
};

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
