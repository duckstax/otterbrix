#pragma once

#include <document/core/value.hpp>

namespace document {

    class wrapper_value_t {
    public:
        explicit wrapper_value_t(const impl::value_t* value)
            : value_(value) {}

        bool operator<(const wrapper_value_t& rhs) const {
           return value_->is_lt(rhs.value_);
        }

        bool operator>(const wrapper_value_t& rhs) const {
            return rhs < *this;
        }

        bool operator<=(const wrapper_value_t& rhs) const {
           return value_->is_lte(rhs.value_);
        }

        bool operator>=(const wrapper_value_t& rhs) const {
            return rhs <= *this;
        }

        bool operator==(const wrapper_value_t& rhs) const {
            return value_->is_equal(rhs.value_);
        }

        bool operator!=(const wrapper_value_t& rhs) const {
            return !(*this == rhs);
        }

        const impl::value_t* operator*() const {
            return value_;
        }

        const impl::value_t* operator->() const {
            return value_;
        }

    private:
        const impl::value_t* value_;
    };


    std::string to_string(const wrapper_value_t &doc) {
        return doc->to_string().as_string();
    }

}