#include <components/storage/database.hpp>

namespace friedrichdb ::core {

    auto database_t::get(const std::string &name) -> friedrichdb::core::collection_t * {
        return storage_.at(name).get();
    }

    auto database_t::get_or_create(const std::string &name) -> collection_t * {
        auto it = storage_.find(name);
        if(it == storage_.end()){
            auto result = storage_.emplace(name, std::make_unique<collection_t>());
            return result.first->second.get();
        } else {
            return it->second.get();
        }

    }

    auto database_t::drop(const std::string &name) -> bool {
        auto it = storage_.find(name);
        if(it ==  storage_.end()){
            return false;
        }else {
            storage_.erase(name);
            return true;
        }

    }

    auto database_t::end() -> database_t::iterator {
        return storage_.end();
    }

    auto database_t::begin() -> database_t::iterator {
        return storage_.begin();
    }

}