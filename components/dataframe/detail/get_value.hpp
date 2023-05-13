#pragma once

#include <cstring>

#include <core/assert/assert.hpp>

#include <dataframe/column/column_view.hpp>
#include <dataframe/traits.hpp>
#include <dataframe/type_dispatcher.hpp>

namespace components::dataframe::detail {

    template<typename T>
    T get_value(column::column_view const& col_view, size_type element_index) {
        assertion_exception_msg(dataframe::is_fixed_width(col_view.type()), "supports only fixed-width types");
        assertion_exception_msg(data_type(type_to_id<T>()) == col_view.type(), "data type mismatch");
        assertion_exception_msg(element_index >= 0 && element_index < col_view.size(), "invalid element_index value");
        T result;
        std::memcpy(&result, col_view.data<T>() + element_index, sizeof(T));
        return result;
    }

} // namespace components::dataframe::detail
