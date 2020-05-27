#pragma once

#include <cstddef>
#include <cstring>

#include <array>
#include <memory>
#include <string>
#include <unordered_map>


namespace services {

    constexpr int64_t kUniqueIDSize = 16;

    class object_id final {
    public:

        bool operator==(const object_id&rhs) const;

        const uint8_t *data() const;

        uint8_t *mutable_data();

        std::string binary() const;

        std::string hex() const;

        size_t hash() const;

        int64_t size();

    private:
        std::array<uint8_t,kUniqueIDSize> id_;
    };

    object_id from_binary(const std::string &binary);

    static_assert(std::is_pod<object_id>::value, "UniqueID must be plain old data");
}

namespace std {
    template<>
    struct hash<::services::object_id> {
        size_t operator()(const ::services::object_id&id) const { return id.hash(); }
    };
}


