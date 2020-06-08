#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage_engine.hpp"

namespace services {

    class memory_hash_storage final : public storage {
    public:
        memory_hash_storage() = default;

        void connect(const std::string& endpoint) override;

        void get(temporary_buffer_storage& buffers) override;

        void put(temporary_buffer_storage& data) override;

    private:
        using hash_table_t = std::unordered_map<object_id, std::unique_ptr<buffer_tt>>;

        hash_table_t table_;
    };

} // namespace services