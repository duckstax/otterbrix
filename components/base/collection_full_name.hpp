#pragma once
// todo: pmr

#include <sstream>
#include <string>

using uid_name_t = std::string;
using database_name_t = std::string;
using schema_name_t = std::string;
using collection_name_t = std::string;

struct collection_full_name_t {
    uid_name_t uid;
    database_name_t database;
    schema_name_t schema;
    collection_name_t collection;
    collection_full_name_t() = default;

    collection_full_name_t(const database_name_t& database, const collection_name_t& collection)
        : database(database)
        , collection(collection) {}

    collection_full_name_t(const database_name_t& database,
                           const schema_name_t& schema,
                           const collection_name_t& collection)
        : database(database)
        , schema(schema)
        , collection(collection) {}

    collection_full_name_t(const uid_name_t& uid,
                           const database_name_t& database,
                           const schema_name_t& schema,
                           const collection_name_t& collection)
        : uid(uid)
        , database(database)
        , schema(schema)
        , collection(collection) {}

    inline std::string to_string() const {
        std::stringstream s;
        if (empty()) {
            s << "NonCollectionData";
        } else {
            s << database << "." << collection;
        }
        return s.str();
    }

    bool empty() const noexcept { return uid.empty() && database.empty() && schema.empty() && collection.empty(); }
};

inline bool operator==(const collection_full_name_t& c1, const collection_full_name_t& c2) {
    return c1.uid == c2.uid && c1.database == c2.database && c1.schema == c2.schema && c1.collection == c2.collection;
}

inline bool operator<(const collection_full_name_t& c1, const collection_full_name_t& c2) {
    return c1.uid < c2.uid || c1.database < c2.database || (c1.database == c2.database && c1.schema < c2.schema) ||
           (c1.database == c2.database && c1.schema == c2.schema && c1.collection < c2.collection);
}

struct collection_name_hash {
    inline std::size_t operator()(const collection_full_name_t& key) const {
        return std::hash<std::string>()(key.uid) ^ std::hash<std::string>()(key.database) ^
               std::hash<std::string>()(key.schema) ^ std::hash<std::string>()(key.collection);
    }
};
