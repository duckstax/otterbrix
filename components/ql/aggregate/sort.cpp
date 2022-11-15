#include "sort.hpp"
#include <sstream>

namespace components::ql::aggregate {

    void append_sort(sort_t& sort, const ql::key_t& key, sort_order order) {
        sort.values.push_back({key, order});
    }

#ifdef DEV_MODE
    std::string debug(const sort_t &sort) {
        std::stringstream stream;
        stream << sort;
        return stream.str();
    }
#endif

} // namespace components::ql::aggregate
