#include "wrapper_value.hpp"
#include <components/document/mutable/mutable_value.hpp>

namespace document {

    std::string to_string(const document::wrapper_value_t& doc) {
        if (doc->type() == impl::value_type::string) {
            return "\"" + doc->to_string().as_string() + "\"";
        }
        return doc->to_string().as_string();
    }

    wrapper_value_t operator+(const wrapper_value_t& value1, const wrapper_value_t& value2) {
        if (!value1.value_) {
            return value2;
        } else if (!value2.value_) {
            return value1;
        } else if (value1.value_->is_double() || value2.value_->is_double()) {
            return wrapper_value_t(impl::new_value(value1.value_->as_double() + value2.value_->as_double()).detach());
        } else if (value1.value_->is_int() || value2.value_->is_int()) {
            return wrapper_value_t(impl::new_value(value1.value_->as_int() + value2.value_->as_int()).detach());
        } else if (value1.value_->is_int() || value2.value_->is_int()) {
            return wrapper_value_t(impl::new_value(value1.value_->as_unsigned() + value2.value_->as_unsigned()).detach());
        }
        return wrapper_value_t(nullptr);
    }

} // namespace document
