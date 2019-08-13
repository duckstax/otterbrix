#pragma once

#include <unordered_map>

#include "file_manager.hpp"

using operand = std::function<std::string(const std::string&)>;

class data_set final {
private:
    using storage_t = std::map<std::size_t, std::string>;
    using const_iterator = storage_t::const_iterator;
public:
    data_set(std::map<std::size_t ,std::string> d):data_(std::move(d)){}

    auto transform(operand&&f){
        for(auto&i:data_){
            i.second = f(i.second);
        }
    }

    auto range() -> std::pair<const_iterator,const_iterator>{
        return std::make_pair(data_.cbegin(),data_.cend());
    }

private:
    std::map<std::size_t, std::string> data_;
};

class context final {
public:
    context(file_manager *fileManager) : file_manager_(fileManager) {}

    auto create_data_set(const std::string&path)->data_set*{
        auto&file = file_manager_->open(path);
        ds_ = std::make_unique<data_set>(file.read());
        return ds_.get();
    }
private:
    file_manager *file_manager_;
    std::unique_ptr<data_set> ds_;
};



class data_set_manager final {
public:
    data_set_manager(file_manager * fm_ptr):file_manager_(fm_ptr){}

    auto create_context(const std::string&name) -> context* {
        auto result = storage_t.emplace(name,std::make_unique<context>(file_manager_));
        return result.first->second.get();
    }
private:
    file_manager *file_manager_;
    std::unordered_map<std::string,std::unique_ptr<context>> storage_t;
};