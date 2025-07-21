#include "operator_get.hpp"

namespace components::table::operators::get {

    types::logical_value_t operator_get_t::value(const std::pmr::vector<types::logical_value_t>& row) {
        return get_value_impl(row);
    }

} // namespace components::table::operators::get
