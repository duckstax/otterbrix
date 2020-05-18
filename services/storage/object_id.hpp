#pragma once

#include <cstddef>
#include <cstring>

#include <array>
#include <memory>
#include <string>
#include <unordered_map>


namespace services {

    constexpr int64_t kUniqueIDSize = 16;

    class UniqueID final {
    public:

        bool operator==(const UniqueID &rhs) const;

        const uint8_t *data() const;

        uint8_t *mutable_data();

        std::string binary() const;

        std::string hex() const;

        size_t hash() const;

        int64_t size();

    private:
        std::array<uint8_t,kUniqueIDSize> id_;
    };

    UniqueID from_binary(const std::string &binary);

    static_assert(std::is_pod<UniqueID>::value, "UniqueID must be plain old data");

    typedef UniqueID ObjectID;
}

namespace std {
    template<>
    struct hash<::services::UniqueID> {
        size_t operator()(const ::services::UniqueID &id) const { return id.hash(); }
    };
}


