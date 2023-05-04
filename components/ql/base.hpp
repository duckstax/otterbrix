#pragma once

#include <string>

using database_name_t = std::string;
using collection_name_t = std::string;

struct collection_full_name_t {
    database_name_t database;
    collection_name_t collection;
};
