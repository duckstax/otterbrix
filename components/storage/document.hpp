#pragma once

#include <memory>
#include <iostream>

#include <nlohmann/json.hpp>

namespace components::storage {

    class document_t final {
    public:
        using json_t = nlohmann::json;
        using json_ptr_t = std::unique_ptr<json_t>;

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
