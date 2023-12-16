#pragma once

#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>
#include <components/document/document.hpp>
#include <components/tests/generaty.hpp>
#include <integration/cpp/base_spaces.hpp>

inline configuration::config test_create_config(const std::filesystem::path& path) {
    auto config = configuration::config::default_config();
    config.log.path = path;
    config.disk.path = path;
    config.wal.path = path;
    // To change log level
    // config.log.level =log_t::level::trace;
    return config;
}

inline void test_clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.disk.path);
    std::filesystem::create_directories(config.disk.path);
}

class test_spaces final : public otterbrix::base_otterbrix_t {
public:
    test_spaces(const configuration::config& config)
        : otterbrix::base_otterbrix_t(config) {}
};

template<class T>
document::retained_t<dict_t> make_dict(const std::string& field, const std::string& key, T value) {
    auto key_value = dict_t::new_dict();
    key_value->set(key, value);
    auto cond = dict_t::new_dict();
    cond->set(field, key_value);
    return cond;
}

template<class T>
document_ptr make_condition(const std::string& field, const std::string& key, T value) {
    auto dict = make_dict(field, key, value);
    return make_document(dict);
}

inline document::retained_t<dict_t> make_dict(const std::string& aggregate,
                                              const std::list<document::retained_t<dict_t>>& sub_dict) {
    auto dict = dict_t::new_dict();
    auto array = array_t::new_array();
    for (const auto& sub_cond : sub_dict) {
        array->append(sub_cond);
    }
    dict->set(aggregate, array);
    return dict;
}

inline document_ptr make_condition(const std::string& aggregate,
                                   const std::list<document::retained_t<dict_t>>& sub_conditions) {
    auto dict = make_dict(aggregate, sub_conditions);
    return make_document(dict);
}
