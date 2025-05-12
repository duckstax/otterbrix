#include "sort.hpp"

namespace services::table::sort {

    sorter_t::sorter_t(size_t index, order order_) { add(index, order_); }

    void sorter_t::add(size_t index, order order_) {
        functions_.emplace_back([index, order_](const std::pmr::vector<components::types::logical_value_t>& vec1,
                                                const std::pmr::vector<components::types::logical_value_t>& vec2) {
            auto k_order = static_cast<int>(order_ == order::ascending ? compare_t::more : compare_t::less);
            return static_cast<compare_t>(k_order * static_cast<int>(vec1.at(index).compare(vec2.at(index))));
        });
    }

} // namespace services::table::sort