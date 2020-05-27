#include <cassert>
#include <memory>
#include <string>

#include "hash_storage.hpp"



namespace services {

    void memory_hash_storage::connect(const std::string& /*endpoint*/) {

    }

    void memory_hash_storage::put(temporary_buffer_storage& data) {
        for (auto& i : data) {
            table_[i.first] = *(i.second);
        }

    }

    void memory_hash_storage::get(const std::vector<object_id>& ids, std::vector<std::unique_ptr<buffer_tt>>& buffers) {
        /*
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
                std::memcpy(buffers[i]->mutable_data(), result->second.data(), result->second.size());
            }
        }
    */
    }

} // namespace services
