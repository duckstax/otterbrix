#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <list>
#include <variant>
#include <memory>

enum class tuple_type : char {
    string_t,
    struct_t,
    void_t,
};

using storage_t =  std::variant<std::uint64_t,std::int64_t,float,double,std::unique_ptr<std::string>>;

class metadata_t final {
public:
    std::uint32_t offset() const {
        return offset_;
    }
private:
    std::uint32_t offset_;
    tuple_type type_;
    int schema_id_;
};

class tuple final {
public:
    tuple(std::uint32_t size){
        metadata_.reserve(size);
        storage_.reserve(size);
    }

    storage_t& get(std::uint32_t index) {
         return storage_.at(metadata_[index].offset());
    }

    std::size_t size_t() const {
        return storage_.size();
    }

private:
    std::vector<metadata_t> metadata_;
    std::vector<storage_t>storage_;
};

tuple make_tuple(std::vector<tuple_type>& types){
    return tuple(types.size());
}

class schema final {
public:
    std::uint32_t name_to_pos(std::string& name){
        return static_cast<uint32_t>(mapper_.at(name).back());
    }
private:
    std::unordered_map<std::string,std::vector<int>> mapper_;
};

class col_t final {
public:

private:
    schema schema_;
    std::unordered_map<std::string,std::uint32_t> object_id_to_pos_;
    std::vector<tuple> storage_;
};

