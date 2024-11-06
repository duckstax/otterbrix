#pragma once

#include <boost/algorithm/string.hpp>
#include <cstdint>
#include <cstdlib>
#include <unordered_map>
#include <unordered_set>

// Jenkins hash function: https://en.wikipedia.org/wiki/Jenkins_hash_function
struct case_insensitive_hash {
    uint64_t operator()(const std::string& str) const {
        uint32_t hash = 0;
        for (auto c : str) {
            hash += static_cast<uint32_t>(std::tolower(static_cast<char>(c)));
            hash += hash << 10;
            hash ^= hash >> 6;
        }
        hash += hash << 3;
        hash ^= hash >> 11;
        hash += hash << 15;
        return hash;
    }
};

struct case_insensitive_equals {
    bool operator()(const std::string& str1, const std::string& str2) const { return boost::iequals(str1, str2); }
};

namespace components::sql_new::transform {
    template<typename T>
    using case_insensitive_map_t = std::unordered_map<std::string, T, case_insensitive_hash, case_insensitive_equals>;

    using case_insensitive_set_t = std::unordered_set<std::string, case_insensitive_hash, case_insensitive_equals>;
} // namespace components::sql_new::transform
