#pragma once

#include <utility>
#include <memory>
#include <algorithm>
#include <unordered_map>
#include <iostream>

#include "document.hpp"

namespace friedrichdb::core {

    class collection_t final {
    public:
        using storage_t = std::unordered_map<std::string, document_ptr>;
        using iterator = typename storage_t::iterator ;

        void insert(const std::string& uid, document_ptr document) {
            storage_.emplace(uid, std::move(document));
        }

        document_t *get(const std::string& uid) {
            auto it = storage_.find(uid);
            if (it == storage_.end()) {
                return nullptr;
            } else {
                it->second.get();
            }
        }

        std::size_t size() const {
            return storage_.size();
        }

        auto begin() -> iterator {
            return storage_.begin();
        }

        auto end() -> iterator {
            return storage_.end();
        }

        auto remove(const std::string& key ){
            storage_.erase(key);
        }

        void drop(){
            storage_.clear();
        }

    private:
        storage_t storage_;
    };

}