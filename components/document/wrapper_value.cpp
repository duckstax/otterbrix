#include "wrapper_value.hpp"
#include <components/document/mutable/mutable_value.hpp>

namespace document {

    std::string to_string(const document::wrapper_value_t& doc) {
        if (doc->type() == impl::value_type::string) {
            std::string tmp;
            tmp.append("\"").append(to_string(*doc)).append("\"");
            return tmp;
        }
        return to_string(*doc);
    }

    wrapper_value_t sum(const wrapper_value_t& value1, const wrapper_value_t& value2) {
        if (!*value1) {
            return value2;
        } else if (!*value2) {
            return value1;
        } else if (value1->is_double() || value2->is_double()) {
            return wrapper_value_t(impl::new_value(value1->as_double() + value2->as_double()).detach());
        } else if (value1->is_int() || value2->is_int()) {
            return wrapper_value_t(impl::new_value(value1->as_int() + value2->as_int()).detach());
        } else if (value1->is_int() || value2->is_int()) {
            return wrapper_value_t(impl::new_value(value1->as_unsigned() + value2->as_unsigned()).detach());
        }
        return wrapper_value_t(nullptr);
    }

} // namespace document
