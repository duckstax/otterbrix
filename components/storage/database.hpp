#pragma once

#include <unordered_map>
#include "collection.hpp"

namespace friedrichdb ::core {

    class database_t final {
    public:
        using storage_t = std::unordered_map<std::string, std::unique_ptr<collection_t>>;
        using iterator = typename storage_t::iterator;

        auto get(const std::string &name) -> collection_t *;

        auto get_or_create(const std::string &name) -> collection_t *;

        auto begin() -> iterator;

        auto end() -> iterator;

        auto drop(const std::string& name) -> bool;

    private:
        storage_t storage_;
    };

}