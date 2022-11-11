#include "sort.hpp"

namespace components::ql::aggregate {

    void append_sort(sort_t& sort, const ql::key_t& key, sort_order order) {
        sort.values.push_back({key, order});
    }

#ifdef DEV_MODE
    std::string debug(const sort_t &sort) {
        std::string value = "$sort: {";
        bool is_first = true;
        for (const auto &v : sort.values) {
            if (is_first) {
                is_first = false;
            } else {
                value += ", ";
            }
            value += v.key.as_string() + ": " + std::to_string(int(v.order));
        }
        value += "}";
        return value;
    }
#endif

} // namespace components::ql::aggregate
