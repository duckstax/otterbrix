#pragma once

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <string>

enum class protocol_op : uint32_t {
    create_collection,
    create_database,
    select,
    insert,
    erase
};


using composition_key_t = std::string;
using query_t = std::string;

using session_id = std::uintptr_t;

struct session_t {
    session_id id_;
    boost::uuids::uuid uid_;
};

struct select_t {
    std::string name_table_;
    std::string key_;
};

struct insert_t final {
    std::string name_table_;
    std::vector<std::string> column_name_;
    std::vector<std::string> values_;
};


struct erase_t {

};