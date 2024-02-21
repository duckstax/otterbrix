#include "slice.hpp"

#include <core/assert/assert.hpp>

#include "dataframe/detail/bitmask.hpp"
#include "make.hpp"
#include <dataframe/column/copy.hpp>
#include <dataframe/column/slice.hpp>
#include <dataframe/detail/get_value.hpp>

namespace components::dataframe::lists {

    std::unique_ptr<column::column_t>
    copy_slice(std::pmr::memory_resource* resource, lists_column_view const& lists, size_type start, size_type end) {
        if (lists.is_empty() or start == end) {
            return column::empty_like(resource, lists.parent());
        }
        if (end < 0 || end > lists.size()) {
            end = lists.size();
        }
        assertion_exception_msg(((start >= 0) && (start < end)), "Invalid slice range.");
        auto lists_count = end - start;
        auto offsets_count = lists_count + 1; // num_offsets always 1 more than num_lists

        // Account for the offset of the view:
        start += lists.offset();
        end += lists.offset();

        // Offsets at the beginning and end of the slice:
        auto offsets_data = lists.offsets().data<size_type>();
        auto start_offset = detail::get_value<size_type>(lists.offsets(), start);
        auto end_offset = detail::get_value<size_type>(lists.offsets(), end);

        core::uvector<size_type> out_offsets(resource, offsets_count);

        // Compute the offsets column of the result:
        std::transform(offsets_data + start, offsets_data + end + 1, out_offsets.data(), [start_offset](size_type i) {
            return i - start_offset;
        });
        auto offsets = std::make_unique<column::column_t>(resource,
                                                          data_type{type_id::int32},
                                                          offsets_count,
                                                          out_offsets.release(),
                                                          core::buffer(resource));

        auto child =
            (lists.child().type() == data_type{type_id::list})
                ? copy_slice(resource, lists_column_view(lists.child()), start_offset, end_offset)
                : std::make_unique<column::column_t>(resource,
                                                     column::slice(lists.child(), {start_offset, end_offset}).front());

        // Compute the null mask of the result:
        auto null_mask = detail::copy_bitmask(resource, lists.null_mask(), start, end);

        return lists::make_lists_column(resource,
                                        lists_count,
                                        std::move(offsets),
                                        std::move(child),
                                        unknown_null_count,
                                        std::move(null_mask));
    }

} // namespace components::dataframe::lists