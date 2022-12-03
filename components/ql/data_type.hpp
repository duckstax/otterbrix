#pragma once

#include "cstdint"

namespace components::ql {

    enum class data_type {
        unknown,
        char_,
        date,
        date_time,
        decimal,
        double_,
        float_,
        int_,
        long_,
        real,
        small_int,
        text,
        time,
        varchar,
    };

    struct doc_type {
        doc_type() = default;
        doc_type(data_type data_type, std::int64_t length = 0, int64_t precision = 0, int64_t scale = 0);
        data_type data_type;
        int64_t length;    // Used for, e.g., VARCHAR(10)
        int64_t precision; // Used for, e.g., DECIMAL (6, 4) or TIME (5)
        int64_t scale;     // Used for DECIMAL (6, 4)
    };

    bool operator==(const doc_type& lhs, const doc_type& rhs);
    bool operator!=(const doc_type& lhs, const doc_type& rhs);

} // namespace components::ql