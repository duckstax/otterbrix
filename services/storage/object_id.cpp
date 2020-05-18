
#include "object_id.hpp"

#include <limits>
#include <utility>
#include <boost/container_hash/hash.hpp>


namespace services {

UniqueID from_binary(const std::string& binary) {
  UniqueID id;
  std::memcpy(&id, binary.data(), sizeof(id));
  return id;
}

const uint8_t* UniqueID::data() const { return id_.data(); }

uint8_t* UniqueID::mutable_data() { return id_.data(); }

std::string UniqueID::binary() const {
  return std::string(id_.begin(), id_.end());
}

std::string UniqueID::hex() const {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < kUniqueIDSize; i++) {
    unsigned int val = id_[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}


size_t UniqueID::hash() const { return boost::hash_value(id_); }

bool UniqueID::operator==(const UniqueID& rhs) const {
  return std::memcmp(data(), rhs.data(), kUniqueIDSize) == 0;
}

    int64_t UniqueID::size() { return kUniqueIDSize; }


}
