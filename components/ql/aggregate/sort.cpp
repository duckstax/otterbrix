#include "sort.hpp"

namespace components::ql::aggregate {

    void append_sort(sort_t& sort, const ql::key_t& key, sort_order order) {
        sort.values.push_back({key, order});
    }

} // namespace components::ql::aggregate
