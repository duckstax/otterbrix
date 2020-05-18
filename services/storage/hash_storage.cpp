

#include <cassert>
#include <memory>
#include <string>


#include "hash_storage.hpp"

namespace services {

bool memory_hash_storage::connect(const std::string& endpoint) { return true; }

bool memory_hash_storage::put(const std::vector<ObjectID>& ids,
                                    const std::vector<std::shared_ptr<components::buffer_t>> &data) {
  for (size_t i = 0; i < ids.size(); ++i) {
    table_[ids[i]] = boost::string_view(*data[i]).to_string();
  }
  return true;
}

bool memory_hash_storage::get(const std::vector<ObjectID>& ids,std::vector<std::shared_ptr<components::buffer_t>> buffers) {
  assert(ids.size() == buffers.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    bool valid;
    hash_table_t::iterator result;
    {
      result = table_.find(ids[i]);
      valid = result != table_.end();
    }
    if (valid) {
      assert(buffers[i]->size() == static_cast<int64_t>(result->second.size()));
      std::memcpy(buffers[i]->mutable_data(), result->second.data(),result->second.size());
    }
  }
  return true;
}


}
