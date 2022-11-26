#pragma once

#include <core/strong_typedef.hpp>
#include <magic_enum.hpp>

STRONG_TYPEDEF(uint16_t, parameter_id_t);

namespace components::expressions {

    using hash_t = std::size_t;

    enum class expression_group : uint8_t {
        invalid,
        compare,
        aggregate,
        scalar
    };

    enum class compare_type : uint8_t {
        invalid,
        eq,
        ne,
        gt,
        lt,
        gte,
        lte,
        regex,
        any,
        all,
        union_and,
        union_or,
        union_not
    };

    enum class aggregate_type : uint8_t {
        invalid,
        count,
        sum,
        min,
        max,
        avg
    };

    enum class scalar_type : uint8_t {
        invalid,
        add,
        subtract,
        multiply,
        divide,
        round,
        ceil,
        floor,
        abs,
        mod,
        pow,
        sqrt
    };

    template <class OStream>
    OStream &operator<<(OStream &stream, const compare_type &type) {
        if (type == compare_type::union_and) {
            stream << "$and";
        } else if (type == compare_type::union_or) {
            stream << "$or";
        } else if (type == compare_type::union_not) {
            stream << "$not";
        } else {
            stream << "$" << magic_enum::enum_name(type);
        }
        return stream;
    }

} // namespace components::expressions
