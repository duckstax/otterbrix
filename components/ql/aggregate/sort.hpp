#pragma once

#include <vector>
#include <components/ql/aggregate/forward.hpp>
#include <components/ql/key.hpp>

namespace components::ql::aggregate {

    enum class sort_order : std::int8_t {
        desc = -1,
        asc = 1
    };

    struct sort_value_t {
        const ql::key_t key;
        const sort_order order;
    };

    struct sort_t final {
        static constexpr operator_type type = operator_type::sort;
        std::vector<sort_value_t> values;
    };

    void append_sort(sort_t& sort, const ql::key_t& key, sort_order order);


    template <class OStream>
    OStream &operator<<(OStream &stream, const sort_t &sort) {
        stream << "$sort: {";
        bool is_first = true;
        for (const auto &v : sort.values) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << v.key << ": " << int(v.order);
        }
        stream << "}";
        return stream;
    }

#ifdef DEV_MODE
    std::string debug(const sort_t &sort);
#endif

} // namespace components::ql::aggregate