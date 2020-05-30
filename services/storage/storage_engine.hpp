#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace services {
    using buffer_tt = std::string;
    using object_id = std::string;
    using temporary_buffer_storage = std::vector<std::pair<const object_id,std::unique_ptr<buffer_tt>>>;

    class storage {
    public:
        storage() = default;

        virtual ~storage() = default;

        virtual void connect(const std::string &endpoint) = 0;

        virtual void put(temporary_buffer_storage&data) = 0;

        virtual void get(temporary_buffer_storage& buffers) = 0;
    };

    std::string extract_storage_name(const std::string &endpoint);

    class storage_engine final {
    public:
        storage_engine() = default;

        using storage_types = std::unordered_map<std::string, std::unique_ptr<storage>> ;

        void register_storage(const std::string &store_name, std::unique_ptr<storage> store);

        void deregister_storage(const std::string &store_name);

        storage& get_store(const std::string &store_name);

    private:
        storage_types store_map_;
    };

}

