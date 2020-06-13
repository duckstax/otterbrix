

#include <iostream>
#include <sstream>

#include "storage_engine.hpp"

namespace services {

    std::string extract_storage_name(const std::string& endpoint) {
        size_t off = endpoint.find_first_of(':');
        if (off == std::string::npos) {
            std::cerr << "Malformed endpoint " << endpoint << std::endl;
            return "";
        }
        return std::string(endpoint.substr(0, off));
    }

    void storage_engine::register_storage(const std::string& store_name, std::unique_ptr<storage> store) {
        store_map_.emplace(store_name, std::move(store));
    }

    void storage_engine::deregister_storage(const std::string& store_name) {
        auto it = store_map_.find(store_name);
        if (it == store_map_.end()) {
            return;
        }
        store_map_.erase(it);
    }

    storage& storage_engine::get_store(const std::string& store_name) {
        auto it = store_map_.find(store_name);
        if (it == store_map_.end()) {
            ///return nullptr;
        } else {
            return *(it->second);
        }
    }

} // namespace services
