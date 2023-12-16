#include "slice.hpp"

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>

#include <vector>

#include "dataframe/detail/bitmask.hpp"
#include "dataframe/detail/get_value.hpp"
#include "make.hpp"
#include <dataframe/detail/get_value.hpp>

namespace components::dataframe::column {

    template<typename UnaryFunction>
    inline auto make_counting_transform_iterator(size_type start, UnaryFunction f) {
        return boost::make_transform_iterator(boost::iterators::make_counting_iterator(start), f);
    }

    std::vector<column::column_view>
    slice(std::pmr::memory_resource* resource, column::column_view const& input, core::span<size_type const> indices) {
        assertion_exception_msg(indices.size() % 2 == 0, "indices size must be even");

        if (indices.empty())
            return {};

        auto iter_start = make_counting_transform_iterator(0, [offset = input.offset(), &indices](size_type index) {
            return indices[index] + offset;
        });
        auto iter_finish =
            make_counting_transform_iterator(indices.size(), [offset = input.offset(), &indices](size_type index) {
                return indices[index] + offset;
            });
        auto null_counts =
            dataframe::detail::segmented_null_count(resource,
                                                    input.null_mask(),
                                                    {iter_start.base().operator->(), iter_finish.base().operator->()});

        auto const children = std::vector<column::column_view>(input.child_begin(), input.child_end());

        auto op = [&](auto i) {
            auto begin = indices[2 * i];
            auto end = indices[2 * i + 1];
            assertion_exception_msg(begin >= 0, "Starting index cannot be negative.");
            assertion_exception_msg(end >= begin, "End index cannot be smaller than the starting index.");
            assertion_exception_msg(end <= input.size(), "Slice range out of bounds.");
            return column::column_view{input.type(),
                                       end - begin,
                                       input.head(),
                                       input.null_mask(),
                                       null_counts[i],
                                       input.offset() + begin,
                                       children};
        };
        auto begin = make_counting_transform_iterator(0, op);
        return std::vector<column::column_view>{begin, begin + indices.size() / 2};
    }

    std::vector<column::column_view> slice(column::column_view const& input, core::span<size_type const> indices) {
        return slice(input, indices);
    }

    std::vector<column::column_view> slice(column::column_view const& input, std::initializer_list<size_type> indices) {
        return slice(input, core::span<size_type const>(indices.begin(), indices.size()));
    }

    std::unique_ptr<column_t> copy_slice(std::pmr::memory_resource* resource,
                                         strings_column_view const& strings,
                                         size_type start,
                                         size_type end) {
        if (strings.is_empty()) {
            return make_empty_column(resource, type_id::string);
        }

        if (end < 0 || end > strings.size()) {
            end = strings.size();
        }
        assertion_exception_msg(((start >= 0) && (start < end)), "Invalid start parameter value.");
        auto const strings_count = end - start;
        auto const offsets_offset = start + strings.offset();

        // slice the offsets child column
        auto offsets_column = std::make_unique<column_t>(
            resource,
            slice(strings.offsets(), {offsets_offset, offsets_offset + strings_count + 1}).front());
        auto const chars_offset =
            offsets_offset == 0 ? 0 : dataframe::detail::get_value<int32_t>(offsets_column->view(), 0);
        if (chars_offset > 0) {
            // adjust the individual offset values only if needed
            auto d_offsets = offsets_column->mutable_view();
            std::transform(d_offsets.begin<int32_t>(),
                           d_offsets.end<int32_t>(),
                           d_offsets.begin<int32_t>(),
                           [chars_offset](auto offset) { return offset - chars_offset; });
        }

        // slice the chars child column
        auto const data_size = dataframe::detail::get_value<int32_t>(offsets_column->view(), strings_count);
        auto chars_column =
            std::make_unique<column_t>(resource,
                                       slice(strings.chars(), {chars_offset, chars_offset + data_size}).front());

        // slice the null mask
        auto null_mask = dataframe::detail::copy_bitmask(resource,
                                                         strings.null_mask(),
                                                         offsets_offset,
                                                         offsets_offset + strings_count);

        return make_strings_column(resource,
                                   strings_count,
                                   std::move(offsets_column),
                                   std::move(chars_column),
                                   unknown_null_count,
                                   std::move(null_mask));
    }

} // namespace components::dataframe::column