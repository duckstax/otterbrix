#include "operator_get.hpp"

namespace services::table::operators::get {

    components::types::logical_value_t
    operator_get_t::value(const std::pmr::vector<components::types::logical_value_t>& row) {
        return get_value_impl(row);
    }

} // namespace services::table::operators::get
