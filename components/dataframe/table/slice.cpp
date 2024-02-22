#include <dataframe/column/column_view.hpp>
#include <dataframe/table/slice.hpp>

#include <memory>
#include <memory_resource>
#include <vector>

#include "dataframe/bitmask.hpp"
#include "dataframe/detail/bitmask.hpp"
#include "table_view.hpp"

#include <boost/iterator/counting_iterator.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator/zip_iterator.hpp>

namespace components::dataframe::table {

    namespace detail {
        std::pmr::vector<column::column_view> slice(std::pmr::memory_resource* resource,
                                                    column::column_view const& input,
                                                    core::span<size_type const> indices) {
            assertion_exception_msg(indices.size() % 2 == 0, "indices size must be even");

            if (indices.empty()) {
                return {};
            }

            //auto indices_iter = boost::iterators::make_transform_iterator(boost::iterators::make_counting_iterator(0), [offset = input.offset(), &indices](size_type index) { return indices[index] + offset; });
            //auto null_counts = components::dataframe::detail::segmented_null_count(resource, input.null_mask(), indices_iter, indices_iter + indices.size());
            auto null_counts =
                components::dataframe::detail::segmented_null_count(resource, input.null_mask(), indices);

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
            auto begin = boost::iterators::make_transform_iterator(boost::iterators::make_counting_iterator(0), op);
            return std::pmr::vector<column::column_view>{begin, begin + indices.size() / 2};
        }

        std::pmr::vector<table_view>
        slice(std::pmr::memory_resource* resource, table_view const& input, core::span<size_type const> indices) {
            assertion_exception_msg(indices.size() % 2 == 0, "indices size must be even");
            if (indices.empty()) {
                return {};
            }

            auto op = [&indices, &resource](auto const& c) { return detail::slice(resource, c, indices); };
            auto f = boost::iterators::make_transform_iterator(input.begin(), op);

            auto sliced_table = std::pmr::vector<std::pmr::vector<column::column_view>>(f, f + input.num_columns());
            sliced_table.reserve(indices.size() + 1);
            std::pmr::vector<table_view> result{};
            size_t num_output_tables = indices.size() / 2;
            for (size_t i = 0; i < num_output_tables; i++) {
                std::vector<column::column_view> table_columns; //todo: pmr
                for (size_type j = 0; j < input.num_columns(); j++) {
                    table_columns.emplace_back(sliced_table[j][i]);
                }
                result.emplace_back(table_view{table_columns});
            }

            return result;
        }

        std::pmr::vector<table_view>
        slice(std::pmr::memory_resource* resource, table_view const& input, std::initializer_list<size_type> indices) {
            return slice(resource, input, core::span<size_type const>(indices.begin(), indices.size()));
        }

    } // namespace detail

    std::pmr::vector<table_view>
    slice(std::pmr::memory_resource* resource, table_view const& input, core::span<size_type const> indices) {
        return detail::slice(resource, input, indices);
    }

    std::pmr::vector<table_view>
    slice(std::pmr::memory_resource* resource, table_view const& input, std::initializer_list<size_type> indices) {
        return detail::slice(resource, input, indices);
    }

} // namespace components::dataframe::table