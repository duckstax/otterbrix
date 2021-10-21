#pragma once

#include <memory>
#include <iostream>
#include <sstream>
#include <nlohmann/json.hpp>
#include "storage/support/ref_counted.hpp"

namespace storage::impl {
class mutable_dict_t;
}

namespace components::storage {

enum class value_type {
    undefined = -1,
    null = 0,
    boolean,
    long_number,
    double_number,
    string,
    array,
    dict
};

using offset_t = std::size_t;
using version_t = uint16_t; //todo

constexpr version_t default_version = 0;
constexpr offset_t not_offset = 0;

struct field_t {
    version_t version;
    value_type type;
    offset_t offset;
    offset_t size;

    field_t(version_t version, value_type type, offset_t offset = not_offset, offset_t size = not_offset);

    bool is_null() const;
    static field_t null();

private:
    bool null_;
};
constexpr const char *key_version = "v";
constexpr const char *key_type    = "t";
constexpr const char *key_offset  = "o";
constexpr const char *key_size    = "s";


class st_document_t final {
public:
    st_document_t(std::stringstream *data = nullptr);
    st_document_t(st_document_t *parent);
    ~st_document_t();

    void add_null(std::string &&key);
    void add_bool(std::string &&key, bool value);
    void add_long(std::string &&key, long value);
    void add_double(std::string &&key, double value);
    void add_string(std::string &&key, std::string &&value);
    void add_dict(std::string &&key, st_document_t &&dict);
//    void add_array(std::string &&key);

    bool is_exists(std::string &&key) const;
    bool is_null(std::string &&key) const;
    bool is_bool(std::string &&key) const;
    bool is_long(std::string &&key) const;
    bool is_double(std::string &&key) const;
    bool is_string(std::string &&key) const;
    bool is_dict(std::string &&key) const;
    bool is_array(std::string &&key) const;

    bool as_bool(std::string &&key) const;
    long as_long(std::string &&key) const;
    double as_double(std::string &&key) const;
    std::string as_string(std::string &&key) const;
    st_document_t as_dict(std::string &&key) const;
//    void as_array(std::string &&key) const; //todo

    //todo call as-functions
    //todo parse complex key
//    template<class T>
//    T get_as(const std::string &key) const {
//    }

    std::string to_json_index() const;

private:
    ::storage::retained_t<::storage::impl::mutable_dict_t> index_;
    std::stringstream *data_;
    bool is_owner_data_;

    void add_index_(std::string &&key, const field_t &field);
    template <class T> void add_value_(std::string &&key, T value, value_type type);

    bool is_type_(std::string &&key, value_type type) const;
    field_t get_index_(std::string &&key) const;
    template <class T> T get_value_(offset_t offset, offset_t size) const;
};





//////////////////////////// OLD ////////////////////////////
    class document_t final {
    public:
        using json_t = nlohmann::json;
        using json_ptr_t = std::unique_ptr<json_t>;
        using const_iterator = json_t::const_iterator;

        static auto  to_array() {
            return document_t(json_t::array());
        }

        document_t();

        document_t(json_t value);

        void add(std::string &&key);

        void add(std::string &&key, bool value);

        void add(std::string &&key, long value);

        void add(std::string &&key, double value);

        void add(std::string &&key, const std::string &value);

        void add(std::string &&key, document_t value) {
            auto tmp = std::move(value);
            json_.emplace(std::move(key), tmp.extract());
        }

        void append(bool value){
            json_.emplace_back(value);
        }
        void append(long value){
            json_.emplace_back(value);
        }
        void append(double value){
            json_.emplace_back(value);
        }

        void append(document_t value){
            auto tmp = std::move(value);
            json_.emplace_back(tmp.extract());
        }

        template<class Char>
        void append(const std::basic_string<Char>& value){
            json_.emplace_back(value);
        }


        document_t(bool value){
            json_= json_t(value);
        }
        document_t(long value){
            json_= json_t(value);
        }
        document_t(double value){
            json_= json_t(value);
        }

        template<class Char>
        document_t(const std::basic_string<Char>& value){
            json_= json_t(value);
        }

        /*
        document_t(document_t value){
            auto tmp = std::move(value);
            json_= json_t(tmp.extract());
        }*/
        template<class T>
        T get_as(std::string key) const{
            //std::cerr << json_[std::move(key)].type_name() << std::endl;
            return json_[std::move(key)].get<T>();
        }

        void append(const std::string& key,bool value){
            if(json_.contains(key)){
                json_[key]=json_t::array();
            }
            json_[key].emplace_back(value);
        }
        void append(const std::string& key,long value){
            if(json_.contains(key)){
                json_[key]=json_t::array();
            }
            json_[key].emplace_back(value);
        }
        void append(const std::string& key,double value){
            if(json_.contains(key)){
                json_[key]=json_t::array();
            }
            json_[key].emplace_back(value);
        }

        template<class Char>
        void append(const std::string& key,const std::basic_string<Char>& value){
            if(json_.contains(key)){
                json_[key]=json_t::array();
            }
            json_[key].emplace_back(value);
        }

        template<class Char>
        void update (const std::basic_string<Char>& key){
            json_[std::move(key)]= nullptr;
        }

        template<class Char>
        void update(const std::basic_string<Char>& key, bool value) {
            json_[key]=value;
        }

        template<class Char>
        void update(const std::basic_string<Char>& key, long value){
            json_[key]=value;
        }

        template<class Char>
        void update(const std::basic_string<Char>& key, double value){
            json_[std::move(key)]=value;
        }

        template<class Char>
        void update(const std::basic_string<Char>& key, const std::string &value){
            json_[std::move(key)]=value;
        }

        const_iterator cbegin() const {
            return json_.cbegin();
        }

        const_iterator cend() const {
            return json_.cend();
        }

        template<class Char>
        bool contains_key(const std::basic_string<Char>& key) const {
            return json_.contains(key);
        }

        auto extract() -> json_t {
            return json_;
        }


        [[nodiscard]] auto to_string() const -> std::string;

        auto get(const std::string &name) -> document_t;

        [[nodiscard]] bool as_bool() const;

        [[nodiscard]] std::string as_string() const;

        [[nodiscard]] double as_double() const;

        [[nodiscard]] int32_t as_int32() const;

        [[nodiscard]] json_t::array_t as_array() const;

        [[nodiscard]] bool is_null() const noexcept;

        [[nodiscard]] bool is_boolean() const noexcept;

        [[nodiscard]] bool is_integer() const noexcept;

        [[nodiscard]] bool is_float() const noexcept;

        [[nodiscard]] bool is_string() const noexcept;

        [[nodiscard]] bool is_array() const noexcept;

        [[nodiscard]] bool is_object() const noexcept;
    private:
        json_t json_;
    };


    using document_ptr = std::unique_ptr<document_t>;

    auto make_document() -> document_ptr;

}
