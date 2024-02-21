#pragma once

#include <algorithm>
#include <memory>
#include <memory_resource>

#include "column_view.hpp"
#include "dataframe/types.hpp"
#include <dataframe/forward.hpp>

#include <core/assert/assert.hpp>

namespace components::dataframe::column {

    template<typename ColumnView>
    ColumnView slice(ColumnView const& input, size_type begin, size_type end) {
        static_assert(std::is_same_v<ColumnView, column::column_view> or
                          std::is_same_v<ColumnView, column::mutable_column_view>,
                      "slice can be performed only on column_view and mutable_column_view");

        assertion_exception_msg(begin >= 0, "Invalid beginning of range.");
        assertion_exception_msg(end >= begin, "Invalid end of range.");
        assertion_exception_msg(end <= input.size(), "Slice range out of bounds.");

        std::vector<ColumnView> children{};
        children.reserve(input.num_children());
        for (size_type index = 0; index < input.num_children(); index++) {
            children.emplace_back(input.child(index));
        }

        return ColumnView(input.type(),
                          end - begin,
                          input.head(),
                          input.null_mask(),
                          unknown_null_count,
                          input.offset() + begin,
                          children);
    }

    std::unique_ptr<column::column_t> empty_like(std::pmr::memory_resource* resource, column::column_view const& input);

} // namespace components::dataframe::column