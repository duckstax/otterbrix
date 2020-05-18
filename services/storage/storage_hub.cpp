

#include <iostream>
#include <sstream>

#include "storage_hub.hpp"

namespace services {

bool storage_hub::extract_storage_name(const std::string& endpoint, std::string* store_name) {
  size_t off = endpoint.find_first_of(':');
  if (off == std::string::npos) {
      std::cerr << "Malformed endpoint " << endpoint <<std::endl;
    return false;
  }
  *store_name = endpoint.substr(0, off);
  return true;
}

void storage_hub::register_storage(const std::string& store_name, std::unique_ptr<storage> store) {
    store_map_.emplace(store_name, std::move(store));
}

void storage_hub::deregister_storage(const std::string& store_name) {
  auto it = store_map_.find(store_name);
  if (it == store_map_.end()) {
    return;
  }
    store_map_.erase(it);
}

storage& storage_hub::GetStore(const std::string& store_name) {
  auto it = store_map_.find(store_name);
  if (it == store_map_.end()) {
    ///return nullptr;
  }
  return *(it->second);
}


}
