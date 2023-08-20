#pragma once
// todo: pmr

#include <string>

using database_name_t = std::string;
using collection_name_t = std::string;

struct collection_full_name_t {
    database_name_t database;
    collection_name_t collection;

    inline std::string_view to_string() const {
        return collection;
    }
};

inline bool operator==(const collection_full_name_t& c1, const collection_full_name_t& c2) {
    return c1.database == c2.database && c1.collection == c2.collection;
}

inline bool operator<(const collection_full_name_t& c1, const collection_full_name_t& c2) {
    return c1.database < c2.database ||
           (c1.database == c2.database && c1.collection < c2.collection);
}
