#pragma once

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <components/document/wrapper_value.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <components/document/document.hpp>

namespace components::ql {

    enum class condition_type : std::uint8_t {
        novalid,
        eq,
        ne,
        gt,
        lt,
        gte,
        lte,
        regex,
        any,
        all,
        union_and,
        union_or,
        union_not
    };

    bool is_union_condition(condition_type type);

    class key_t final {
    public:
        key_t()
            : type_(type::non)
            , storage_({}) {}

        key_t(std::string_view str)
            : type_(type::string)
            , storage_(std::string(str.data(), str.size())) {}

        key_t(const std::string& str)
            : type_(type::string)
            , storage_(std::string(str.data(), str.size())) {}

        key_t(std::string&& str)
            : type_(type::string)
            , storage_(std::move(str)) {}

        key_t(const char* str)
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
        std::variant<std::monostate,bool, int32_t, uint32_t, std::string> storage_;
    };

    using expr_value_t = ::document::wrapper_value_t;

    struct expr_t {
        expr_t(const expr_t&) = delete;
        expr_t(expr_t&&) /*noexcept(false)*/ = default;

        using ptr = std::unique_ptr<expr_t>;

        condition_type type_;
        key_t key_;
        expr_value_t value_;
        std::vector<ptr> sub_conditions_;

        expr_t(condition_type type, std::string key, expr_value_t value);
        explicit expr_t(bool is_union);
        bool is_union() const;
        void append_sub_condition(ptr sub_condition);

    private:
        bool union_;
    };

    using expr_ptr = expr_t::ptr;

    template<class Value>
    inline expr_ptr make_expr(condition_type condition, std::string key, Value value) {
        return make_expr(condition, std::move(key), ::document::impl::new_value(value).detach());
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, expr_value_t value) {
        return std::make_unique<expr_t>(condition, std::move(key), value);
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, const ::document::impl::value_t* value) {
        return make_expr(condition, std::move(key), expr_value_t(value));
    }

    template<>
    inline expr_ptr make_expr(condition_type condition, std::string key, const std::string &value) {
        return make_expr(condition, std::move(key), ::document::impl::new_value(value).detach());
    }

    inline expr_ptr make_expr() {
        return std::make_unique<expr_t>(false);
    }

    inline expr_ptr make_union_expr() {
        return std::make_unique<expr_t>(true);
    }

    std::string to_string(condition_type type);
    std::string to_string(const expr_ptr& expr);

    ::document::wrapper_value_t get_value(const components::document::document_ptr &doc, const key_t &key);

} // namespace components::ql