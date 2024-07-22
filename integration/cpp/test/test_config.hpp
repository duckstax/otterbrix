#pragma once

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
document_ptr
make_condition(std::pmr::memory_resource* resource, const std::string& field, const std::string& key, T value) {
    auto result = make_document(resource);
    result->set_dict(field);
    result->get_dict(field)->set(key, value);
    return result;
}

inline document_ptr make_condition(std::pmr::memory_resource* resource,
                                   const std::string& aggregate,
                                   const std::list<document_ptr>& sub_conditions) {
    auto result = make_document(resource);
    result->set_array(aggregate);
    auto array = result->get_array(aggregate);
    for (const auto& sub_cond : sub_conditions) {
        array->set(aggregate, sub_cond);
    }
    return result;
}
