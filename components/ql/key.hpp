#pragma once

#include <string>
#include <variant>

namespace components::ql {
    class key_t final {
    public:
        key_t()
            : type_(type::non)
            , storage_({}) {}

        explicit key_t(std::string_view str)
            : type_(type::string)
            , storage_(std::string(str.data(), str.size())) {}

        explicit key_t(const std::string& str)
            : type_(type::string)
            , storage_(std::string(str.data(), str.size())) {}

        explicit key_t(std::string&& str)
            : type_(type::string)
            , storage_(std::move(str)) {}

        explicit key_t(const char* str)
            : type_(type::string)
            , storage_(std::string(str)) {}

        template<typename CharT>
        key_t(const CharT* data, size_t size)
            : type_(type::string)
            , storage_(std::string(data, size)) {}

        enum class type {
            non,
            string,
            int32,
            uint32
        };

        auto as_string() const -> const std::string& {
            return std::get<std::string>(storage_);
        }

        explicit operator std::string() const {
            return as_string();
        }

        type which() const {
            return type_;
        }

        auto is_string() const -> bool {
            return type_ == type::string;
        }

        auto is_null() const -> bool {
            return type_ == type::non;
        }

        bool operator<(const key_t& other) const {
            return storage_ < other.storage_;
        }

        bool operator<=(const key_t& other) const {
            return storage_ <= other.storage_;
        }

        bool operator>(const key_t& other) const {
            return storage_ > other.storage_;
        }

        bool operator>=(const key_t& other) const {
            return storage_ >= other.storage_;
        }

        bool operator==(const key_t& other) const {
            return storage_ == other.storage_;
        }

        bool operator!=(const key_t& rhs) const {
            return !(*this == rhs);
        }

    private:
        type type_;
        std::variant<std::monostate, bool, int32_t, uint32_t, std::string> storage_;
    };


    template <class OStream>
    OStream &operator<<(OStream &stream, const key_t &key) {
        if (key.is_string()) {
            stream << key.as_string();
        }
        return stream;
    }

} // namespace components::ql