#include "insert.hpp"

insert_t::insert_t(std::string&& name_table, std::vector<std::string>&& column_name, std::vector<std::string>&& values)
    : name_table_(std::move(name_table))
    , column_name_(std::move(column_name))
    , values_(std::move(values)) {
}