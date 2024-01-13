#pragma once

#include <components/document/core/dict.hpp>
#include <components/document/document.hpp>
#include <cstdint>

namespace components::document {

    enum class compare_t { less = -1, equals = 0, more = 1 };

    class document_view_t final {
    public:
        using const_value_ptr = const ::document::impl::value_t*;

        document_view_t();
        explicit document_view_t(document_ptr document);
        explicit document_view_t(document_view_t&& doc_view) noexcept;

        document_id_t id() const;
        document_ptr get_ptr() const;

        bool is_valid() const;
        bool is_dict() const;
        bool is_array() const;
        std::size_t count() const;

        bool is_exists(const std::string& key) const;
        bool is_exists(std::string_view key) const;
        bool is_exists(uint32_t index) const;
        bool is_null(const std::string& key) const;
        bool is_null(uint32_t index) const;
        bool is_bool(const std::string& key) const;
        bool is_bool(uint32_t index) const;
        bool is_ulong(const std::string& key) const;
        bool is_ulong(uint32_t index) const;
        bool is_long(const std::string& key) const;
        bool is_long(uint32_t index) const;
        bool is_double(const std::string& key) const;
        bool is_double(uint32_t index) const;
        bool is_string(const std::string& key) const;
        bool is_string(uint32_t index) const;
        bool is_array(std::string_view key) const;
        bool is_array(uint32_t index) const;
        bool is_dict(std::string_view key) const;
        bool is_dict(uint32_t index) const;

        const_value_ptr get(const std::string& key) const;
        const_value_ptr get(std::string_view key) const;
        const_value_ptr get(uint32_t index) const;

        bool get_bool(const std::string& key) const;
        uint64_t get_ulong(const std::string& key) const;
        int64_t get_long(const std::string& key) const;
        double get_double(const std::string& key) const;
        std::string get_string(const std::string& key) const;
        document_view_t get_array(std::string_view key) const;
        document_view_t get_array(uint32_t index) const;
        document_view_t get_dict(std::string_view key) const;
        document_view_t get_dict(uint32_t index) const;

        const_value_ptr get_value() const;
        const_value_ptr get_value(std::string_view key) const;
        const_value_ptr get_value(uint32_t index) const;

        template<class T>
        T get_as(const std::string& key) const {
            const auto* value = get(key);
            if (value) {
                return value->as<T>();
            }
            return T();
        }

        template<class T>
        T get_as(uint32_t index) const {
            const auto* value = get(index);
            if (value) {
                return value->as<T>();
            }
            return T();
        }

        ::document::impl::dict_iterator_t begin() const;

        compare_t compare(const document_view_t& other, const std::string& key) const;

        std::string to_json() const;
        const ::document::impl::dict_t* as_dict() const;
        const ::document::impl::array_t* as_array() const;
        ::document::retained_t<::document::impl::dict_t> to_dict() const;
        ::document::retained_t<::document::impl::array_t> to_array() const;

    private:
        document_ptr document_;

        std::string to_json_dict() const;
        std::string to_json_array() const;
    };

} // namespace components::document
