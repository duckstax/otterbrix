#include "simple_value.hpp"

namespace services::table::operators::get {

    operator_get_ptr simple_value_t::create(const components::expressions::key_t& key) {
        return operator_get_ptr(new simple_value_t(key));
    }

    simple_value_t::simple_value_t(const components::expressions::key_t& key)
        : operator_get_t()
        , key_(key) {}

    components::types::logical_value_t
    simple_value_t::get_value_impl(const std::pmr::vector<components::types::logical_value_t>& row) {
        auto it = std::find_if(row.begin(), row.end(), [&](const components::types::logical_value_t& v) {
            return v.type().alias() == key_.as_string();
        });
        if (it == row.end()) {
            return components::types::logical_value_t{nullptr};
        }
        return *it;
    }

} // namespace services::table::operators::get
