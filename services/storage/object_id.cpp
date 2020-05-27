
#include "object_id.hpp"

#include <boost/container_hash/hash.hpp>
#include <limits>
#include <utility>

namespace services {

    object_id from_binary(const std::string& binary) {
        object_id id;
        std::memcpy(&id, binary.data(), sizeof(id));
        return id;
    }

    const uint8_t* object_id::data() const { return id_.data(); }

    uint8_t* object_id::mutable_data() { return id_.data(); }

    std::string object_id::binary() const {
        return std::string(id_.begin(), id_.end());
    }

    std::string object_id::hex() const {
        constexpr char hex[] = "0123456789abcdef";
        std::string result;
        for (int i = 0; i < kUniqueIDSize; i++) {
            unsigned int val = id_[i];
            result.push_back(hex[val >> 4]);
            result.push_back(hex[val & 0xf]);
        }
        return result;
    }

    size_t object_id::hash() const { return boost::hash_value(id_); }

    bool object_id::operator==(const object_id& rhs) const {
        return std::memcmp(data(), rhs.data(), kUniqueIDSize) == 0;
    }

    int64_t object_id::size() { return kUniqueIDSize; }

} // namespace services
