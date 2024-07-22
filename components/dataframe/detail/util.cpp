#include "util.hpp"

#include <boost/iterator/counting_iterator.hpp>
#include <dataframe/column/strings_column_view.hpp>
#include <dataframe/lists/lists_column_view.hpp>
#include <dataframe/table/table.hpp>
#include <dataframe/table/table_view.hpp>

namespace components::dataframe::detail {

    namespace {

        constexpr bool type_may_have_nonempty_nulls(type_id type) {
            return type == type_id::string || type == type_id::list || type == type_id::structs;
        }

        bool has_nonempty_null_rows(const column::column_view& input, std::pmr::memory_resource* resource) {
            if (!input.has_nulls(resource)) {
                return false;
            }

            auto const type = input.type().id();
            auto const offsets = (type == type_id::string) ? (column::strings_column_view{input}).offsets()
                                                           : (lists::lists_column_view{input}).offsets();
            auto const row_begin = boost::counting_iterator<size_type>(0);
            auto const row_end = row_begin + input.size();
            return std::count_if(row_begin, row_end - 1, [&offsets](const size_type& row) {
                       return offsets.begin<size_type>()[row] != offsets.begin<size_type>()[row + 1];
                   }) > 0;
        }

        bool has_nonempty_nulls(column::column_view const& input, std::pmr::memory_resource* resource) {
            auto const type = input.type().id();
            if (!type_may_have_nonempty_nulls(type)) {
                return false;
            }
            if ((type == type_id::string || type == type_id::list) && has_nonempty_null_rows(input, resource)) {
                return true;
            }
            if ((type == type_id::structs || type == type_id::list) &&
                std::any_of(input.child_begin(), input.child_end(), [resource](const auto& child) {
                    return has_nonempty_nulls(child, resource);
                })) {
                return true;
            }
            return false;
        }

        template<typename element_t, typename enable = void>
        struct column_gatherer_impl {
            template<typename... Args>
            std::unique_ptr<column::column_t> operator()(Args&&...) {
                assertion_exception_msg(true, "Unsupported type in gather.");
            }
        };

        struct column_gatherer {
            template<typename element_t, typename iterator>
            std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                         const column::column_view& source_column,
                                                         iterator gather_map_begin,
                                                         iterator gather_map_end,
                                                         bool nullify_out_of_bounds) {
                column_gatherer_impl<element_t> gatherer{};
                return gatherer(resource, source_column, gather_map_begin, gather_map_end, nullify_out_of_bounds);
            }
        };

        enum class gather_bitmask_op
        {
            dont_check,
            passthrough,
            nullify
        };

        template<typename iterator>
        void gather_bitmask(std::pmr::memory_resource* resource,
                            const table::table_view& source,
                            iterator gather_map,
                            std::vector<std::unique_ptr<column::column_t>>& target,
                            gather_bitmask_op op) {
            //todo: impl
        }

        template<typename iterator>
        std::unique_ptr<table::table_t> gather(std::pmr::memory_resource* resource,
                                               const table::table_view& source_table,
                                               iterator gather_map_begin,
                                               iterator gather_map_end,
                                               gather_bitmask_op op) {
            std::vector<std::unique_ptr<column::column_t>> dst;

            for (const auto& source_column : source_table) {
                dst.push_back(type_dispatcher<dispatch_storage_type>(source_column.type(),
                                                                     column_gatherer{},
                                                                     resource,
                                                                     source_column,
                                                                     gather_map_begin,
                                                                     gather_map_end,
                                                                     op == gather_bitmask_op::nullify));
            }

            auto nullable =
                std::any_of(source_table.begin(), source_table.end(), [](auto const& col) { return col.nullable(); });
            gather_bitmask(resource, source_table, gather_map_begin, dst, nullable ? gather_bitmask_op::nullify : op);
            return std::make_unique<table::table_t>(std::move(dst));
        }

        std::unique_ptr<column::column_t> purge_nonempty_nulls(std::pmr::memory_resource* resource,
                                                               const column::column_view& input) {
            if (!is_compound(input.type())) {
                return std::make_unique<column::column_t>(resource, input);
            }

            auto gathered_table = gather(resource,
                                         table::table_view{{input}},
                                         boost::counting_iterator(0),
                                         boost::counting_iterator(input.size()),
                                         gather_bitmask_op::dont_check);
            return std::move(gathered_table->release().front());
        }

        std::unique_ptr<column::column_t> superimpose_nulls_no_sanitize(std::pmr::memory_resource* resource,
                                                                        const bitmask_type* null_mask,
                                                                        size_type null_count,
                                                                        std::unique_ptr<column::column_t>&& input) {
            //todo: impl
        }

    } // namespace

    std::unique_ptr<column::column_t> superimpose_nulls(std::pmr::memory_resource* resource,
                                                        const bitmask_type* null_mask,
                                                        size_type null_count,
                                                        std::unique_ptr<column::column_t>&& input) {
        input = superimpose_nulls_no_sanitize(resource, null_mask, null_count, std::move(input));
        if (const auto input_view = input->view(); has_nonempty_nulls(input_view, resource)) {
            return purge_nonempty_nulls(resource, input_view);
        }
        return std::move(input);
    }

} // namespace components::dataframe::detail
