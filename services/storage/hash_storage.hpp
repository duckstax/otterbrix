#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <components/buffer/buffer.hpp>

#include "storage_hub.hpp"

namespace services {

    class memory_hash_storage final : public storage {
    public:
        memory_hash_storage() = default;

        bool connect(const std::string &endpoint) override;

        bool get(const std::vector<ObjectID> &ids, std::vector<std::shared_ptr<components::buffer_t>> buffers) override;

        bool put(temporary_buffer_storage &data) override;

    private:
        using hash_table_t = std::unordered_map<ObjectID, std::string>;

        hash_table_t table_;
    };

}