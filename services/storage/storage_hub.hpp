#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <components/buffer/buffer.hpp>
#include "object_id.hpp"


namespace services {
    using buffer_tt = std::string;
    using temporary_buffer_storage = std::vector<std::pair<ObjectID,std::unique_ptr<buffer_tt>>>;

    class storage {
    public:
        storage() = default;

        virtual ~storage() = default;

        virtual bool connect(const std::string &endpoint) = 0;

        virtual bool put(temporary_buffer_storage&data) = 0;

        virtual bool get(const std::vector<ObjectID> &ids, std::vector<std::shared_ptr<components::buffer_t>> buffers) = 0;
    };

    class storage_hub final {
    public:

        storage_hub() = default;

        using storage_types = std::unordered_map<std::string, std::unique_ptr<storage>> ;

        bool extract_storage_name(const std::string &endpoint, std::string *store_name);
        
        void register_storage(const std::string &store_name, std::unique_ptr<storage> store);

        void deregister_storage(const std::string &store_name);

        storage& get_store(const std::string &store_name);

    private:
        storage_types store_map_;
    };

}

