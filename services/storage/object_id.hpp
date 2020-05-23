#pragma once

#include <cstddef>
#include <cstring>

#include <array>
#include <memory>
#include <string>
#include <unordered_map>


namespace services {

    constexpr int64_t kUniqueIDSize = 16;

    class unique_id final {
    public:

        bool operator==(const unique_id&rhs) const;

        const uint8_t *data() const;

        uint8_t *mutable_data();

        std::string binary() const;

        std::string hex() const;

        size_t hash() const;

        int64_t size();

    private:
        std::array<uint8_t,kUniqueIDSize> id_;
    };

    unique_id from_binary(const std::string &binary);

    static_assert(std::is_pod<unique_id>::value, "UniqueID must be plain old data");

    typedef unique_id ObjectID;
}

namespace std {
    template<>
    struct hash<::services::unique_id> {
        size_t operator()(const ::services::unique_id&id) const { return id.hash(); }
    };
}


