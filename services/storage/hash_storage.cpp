#include <cassert>
#include <memory>
#include <string>

#include "hash_storage.hpp"

namespace services {

    void memory_hash_storage::connect(const std::string& /*endpoint*/) {}

    void memory_hash_storage::put(temporary_buffer_storage& data) {
        for (auto& i : data) {
            table_[i.first] = std::move(i.second);
        }
    }

    void memory_hash_storage::get(temporary_buffer_storage& buffers) {
        for (auto& i : buffers) {
            auto result = table_.find(i.first);
            if (result != table_.end()) {
                i.second = std::make_unique<buffer_tt>(*result->second);
            }
        }
    }

} // namespace services
