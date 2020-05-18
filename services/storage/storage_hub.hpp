#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <components/buffer/buffer.hpp>
#include "object_id.hpp"


namespace services {

    class storage {
    public:
        storage() = default;

        virtual ~storage() = default;

        virtual bool connect(const std::string &endpoint) = 0;

        virtual bool put(const std::vector<ObjectID> &ids, const std::vector<std::shared_ptr<components::buffer_t>> &data) = 0;

        virtual bool get(const std::vector<ObjectID> &ids, std::vector<std::shared_ptr<components::buffer_t>> buffers) = 0;
    };

    class storage_hub final {
    public:

        storage_hub() = default;

        using StoreMap = std::unordered_map<std::string, std::unique_ptr<storage>> ;

        bool extract_storage_name(const std::string &endpoint, std::string *store_name);
        
        void register_storage(const std::string &store_name, std::unique_ptr<storage> store);

        void deregister_storage(const std::string &store_name);

        storage& GetStore(const std::string &store_name);

    private:

        StoreMap store_map_;
    };

}

