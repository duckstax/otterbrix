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

        bool has_nonempty_null_rows(const column::column_view& input) {
            if (!input.has_nulls()) {
                return false;
            }

            auto const type = input.type().id();
            auto const offsets = (type == type_id::string)
                                     ? (column::strings_column_view{input}).offsets()
                                     : (lists::lists_column_view{input}).offsets();
            auto const row_begin = boost::counting_iterator<size_type>(0);
            auto const row_end = row_begin + input.size();
            //todo: check
            return std::count_if(row_begin, row_end - 1, [&offsets](const size_type& row) {
                       return offsets.begin<size_type>()[row] != offsets.begin<size_type>()[row + 1];
                   }) > 0;
        }

        bool has_nonempty_nulls(column::column_view const& input) {
            auto const type = input.type().id();
            if (!type_may_have_nonempty_nulls(type)) {
                return false;
            }
            if ((type == type_id::string || type == type_id::list) && has_nonempty_null_rows(input)) {
                return true;
            }
            if ((type == type_id::structs || type == type_id::list) &&
                std::any_of(input.child_begin(), input.child_end(), [](const auto& child) {
                    return has_nonempty_nulls(child);
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
            std::unique_ptr<column::column_t> operator()(
                std::pmr::memory_resource* resource,
                const column::column_view& source_column,
                iterator gather_map_begin,
                iterator gather_map_end,
                bool nullify_out_of_bounds) {
                column_gatherer_impl<element_t> gatherer{};
                return gatherer(resource, source_column, gather_map_begin, gather_map_end, nullify_out_of_bounds);
            }
        };

        template<typename element_t>
        struct column_gatherer_impl<element_t, std::enable_if_t<is_rep_layout_compatible<element_t>()>> {
            template<typename iterator>
            std::unique_ptr<column::column_t> operator()(
                std::pmr::memory_resource* resource,
                const column::column_view& column,
                iterator gather_map_begin,
                iterator gather_map_end,
                bool nullify_out_of_bounds) {
                /*
                auto const num_rows = cudf::distance(gather_map_begin, gather_map_end);
                auto const policy = cudf::mask_allocation_policy::NEVER;
                auto destination_column =
                    cudf::detail::allocate_like(source_column, num_rows, policy, stream, mr);

                gather_helper(source_column.data<element_t>(),
                              source_column.size(),
                              destination_column->mutable_view().template begin<element_t>(),
                              gather_map_begin,
                              gather_map_end,
                              nullify_out_of_bounds,
                              stream);

                return destination_column;
                */
            }
        };

        template<>
        struct column_gatherer_impl<column::strings_column_view> {
            template<typename iterator>
            std::unique_ptr<column::column_t> operator()(
                std::pmr::memory_resource* resource,
                const column::column_view& source_column,
                iterator gather_map_begin,
                iterator gather_map_end,
                bool nullify_out_of_bounds) {
                /*
                if (true == nullify_out_of_bounds) {
                    return strings::detail::gather<true>(
                        strings_column_view(source_column), gather_map_begin, gather_map_end, stream, mr);
                } else {
                    return strings::detail::gather<false>(
                        strings_column_view(source_column), gather_map_begin, gather_map_end, stream, mr);
                }
                */
            }
        };

        template<>
        struct column_gatherer_impl<lists::list_view> {
            template<typename iterator>
            std::unique_ptr<column::column_t> operator()(
                std::pmr::memory_resource* resource,
                const column::column_view& column,
                iterator gather_map_begin,
                iterator gather_map_end,
                bool nullify_out_of_bounds) {
                /*
                lists_column_view list(column);
                auto gather_map_size = std::distance(gather_map_begin, gather_map_end);
                if (gather_map_size == 0) {
                    return empty_like(column);
                }

                lists::detail::gather_data gd = nullify_out_of_bounds
                                                    ? lists::detail::make_gather_data<true>(
                                                          column, gather_map_begin, gather_map_size, stream, mr)
                                                    : lists::detail::make_gather_data<false>(
                                                          column, gather_map_begin, gather_map_size, stream, mr);

                if (list.child().type() == data_type{type_id::LIST}) {
                    auto child = lists::detail::gather_list_nested(list.get_sliced_child(stream), gd, stream, mr);

                    return make_lists_column(gather_map_size,
                                             std::move(gd.offsets),
                                             std::move(child),
                                             0,
                                             rmm::device_buffer{0, stream, mr});
                }

                auto child = lists::detail::gather_list_leaf(list.get_sliced_child(stream), gd, stream, mr);

                return make_lists_column(gather_map_size,
                                         std::move(gd.offsets),
                                         std::move(child),
                                         0,
                                         rmm::device_buffer{0, stream, mr});
                */
            }
        };

        template<>
        struct column_gatherer_impl<dictionary::dictionary32> {
            template<typename iterator>
            std::unique_ptr<column::column_t> operator()(
                std::pmr::memory_resource* resource,
                const column::column_view& source_column,
                iterator gather_map_begin,
                iterator gather_map_end,
                bool nullify_out_of_bounds) {
                /*
                dictionary_column_view dictionary(source_column);
                auto output_count = std::distance(gather_map_begin, gather_map_end);
                if (output_count == 0) return make_empty_column(type_id::DICTIONARY32);
                auto keys_copy = std::make_unique<column>(dictionary.keys(), stream, mr);
                column_view indices = dictionary.get_indices_annotated();
                auto new_indices    = detail::allocate_like(
                    indices, output_count, mask_allocation_policy::NEVER, stream, mr);
                gather_helper(
                    detail::indexalator_factory::make_input_iterator(indices),
                    indices.size(),
                    detail::indexalator_factory::make_output_iterator(new_indices->mutable_view()),
                    gather_map_begin,
                    gather_map_end,
                    nullify_out_of_bounds,
                    stream);
                auto indices_type = new_indices->type();
                auto null_count   = new_indices->null_count();
                auto contents     = new_indices->release();
                auto indices_column = std::make_unique<column>(indices_type,
                                                               static_cast<size_type>(output_count),
                                                               std::move(*(contents.data.release())),
                                                               rmm::device_buffer{0, stream, mr},
                                                               0);
                return make_dictionary_column(std::move(keys_copy),
                                              std::move(indices_column),
                                              std::move(*(contents.null_mask.release())),
                                              null_count);
                */
            }
        };

        template<>
        struct column_gatherer_impl<structs::struct_view> {
            template<typename iterator>
            std::unique_ptr<column::column_t> operator()(
                std::pmr::memory_resource* resource,
                const column::column_view& column,
                iterator gather_map_begin,
                iterator gather_map_end,
                bool nullify_out_of_bounds) {
                /*
                auto const gather_map_size = std::distance(gather_map_begin, gather_map_end);
                if (gather_map_size == 0) { return empty_like(column); }

                std::vector<column_view> sliced_children;
                std::transform(thrust::make_counting_iterator(0),
                               thrust::make_counting_iterator(column.num_children()),
                               std::back_inserter(sliced_children),
                               [structs_view = structs_column_view{column}](auto const idx) {
                                   return structs_view.get_sliced_child(idx);
                               });

                std::vector<std::unique_ptr<column>> output_struct_members;
                std::transform(sliced_children.begin(),
                               sliced_children.end(),
                               std::back_inserter(output_struct_members),
                               [&](auto const& col) {
                                   return type_dispatcher<dispatch_storage_type>(col.type(),
                                                                                       column_gatherer{},
                                                                                       col,
                                                                                       gather_map_begin,
                                                                                       gather_map_end,
                                                                                       nullify_out_of_bounds,
                                                                                       stream,
                                                                                       mr);
                               });

                auto const nullable =
                    nullify_out_of_bounds || std::any_of(sliced_children.begin(),
                                                         sliced_children.end(),
                                                         [](auto const& col) { return col.nullable(); });

                if (nullable) {
                    gather_bitmask(
                        table_view{
                            std::vector<column_view>{sliced_children.begin(), sliced_children.end()}},
                        gather_map_begin,
                        output_struct_members,
                        nullify_out_of_bounds ? gather_bitmask_op::NULLIFY : gather_bitmask_op::DONT_CHECK,
                        stream,
                        mr);
                }

                return make_structs_column(
                    gather_map_size,
                    std::move(output_struct_members),
                    0,
                    rmm::device_buffer{0, stream, mr},
                    stream,
                    mr);
                */
            }
        };

        enum class gather_bitmask_op {
            dont_check,
            passthrough,
            nullify
        };

        template<typename iterator>
        void gather_bitmask(
            std::pmr::memory_resource* resource,
            const table::table_view& source,
            iterator gather_map,
            std::vector<std::unique_ptr<column::column_t>>& target,
            gather_bitmask_op op) {

            //todo
            /*
            if (target.empty()) {
                return;
            }

            auto const target_rows = target.front()->size();
            CUDF_EXPECTS(std::all_of(target.begin(),
                                     target.end(),
                                     [target_rows](auto const& col) { return target_rows == col->size(); }),
                         "Column size mismatch");

            for (size_t i = 0; i < target.size(); ++i) {
                if ((source.column(i).nullable() or op == gather_bitmask_op::NULLIFY) and
                    not target[i]->nullable()) {
                    auto const state =
                        op == gather_bitmask_op::PASSTHROUGH ? mask_state::ALL_VALID : mask_state::UNINITIALIZED;
                    auto mask = detail::create_null_mask(target[i]->size(), state, stream, mr);
                    target[i]->set_null_mask(std::move(mask), 0);
                }
            }

            std::vector<bitmask_type*> target_masks(target.size());
            std::transform(target.begin(), target.end(), target_masks.begin(), [](auto const& col) {
                return col->mutable_view().null_mask();
            });
            auto d_target_masks = make_device_uvector_async(target_masks, stream);

            auto const device_source = table_device_view::create(source, stream);
            auto d_valid_counts = make_zeroed_device_uvector_async<size_type>(target.size(), stream);

            auto const impl = [op]() {
                switch (op) {
                    case gather_bitmask_op::DONT_CHECK:
                        return gather_bitmask<gather_bitmask_op::DONT_CHECK, MapIterator>;
                    case gather_bitmask_op::PASSTHROUGH:
                        return gather_bitmask<gather_bitmask_op::PASSTHROUGH, MapIterator>;
                    case gather_bitmask_op::NULLIFY:
                        return gather_bitmask<gather_bitmask_op::NULLIFY, MapIterator>;
                    default:
                        CUDF_FAIL("Invalid gather_bitmask_op");
                }
            }();
            impl(*device_source,
                 gather_map,
                 d_target_masks.data(),
                 target.size(),
                 target_rows,
                 d_valid_counts.data(),
                 stream);

            auto const valid_counts = make_std_vector_sync(d_valid_counts, stream);
            for (size_t i = 0; i < target.size(); ++i) {
                if (target[i]->nullable()) {
                    auto const null_count = target_rows - valid_counts[i];
                    target[i]->set_null_count(null_count);
                }
            }
            */
        }

        template<typename iterator>
        std::unique_ptr<table::table_t> gather(
            std::pmr::memory_resource* resource,
            const table::table_view& source_table,
            iterator gather_map_begin,
            iterator gather_map_end,
            gather_bitmask_op op) {
            std::vector<std::unique_ptr<column::column_t>> dst;

            for (const auto& source_column : source_table) {
                dst.push_back(
                    type_dispatcher<dispatch_storage_type>(source_column.type(),
                                                           column_gatherer{},
                                                           resource,
                                                           source_column,
                                                           gather_map_begin,
                                                           gather_map_end,
                                                           op == gather_bitmask_op::nullify));
            }

            auto nullable = std::any_of(source_table.begin(), source_table.end(), [](auto const& col) { return col.nullable(); });
            gather_bitmask(resource, source_table, gather_map_begin, dst, nullable ? gather_bitmask_op::nullify : op);
            return std::make_unique<table::table_t>(std::move(dst));
        }

        std::unique_ptr<column::column_t> purge_nonempty_nulls(
            std::pmr::memory_resource* resource,
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

        std::unique_ptr<column::column_t> superimpose_nulls_no_sanitize(
            std::pmr::memory_resource* resource,
            const bitmask_type* null_mask,
            size_type null_count,
            std::unique_ptr<column::column_t>&& input) {
            //todo
            /*
            if (input->type().id() == type_id::EMPTY) {
                // EMPTY columns should not have a null mask,
                // so don't superimpose null mask on empty columns.
                return std::move(input);
            }

            auto const num_rows = input->size();

            if (!input->nullable()) {
                input->set_null_mask(detail::copy_bitmask(null_mask, 0, num_rows, stream, mr));
                input->set_null_count(null_count);
            } else {
                auto current_mask = input->mutable_view().null_mask();
                std::vector<bitmask_type const*> masks{reinterpret_cast<bitmask_type const*>(null_mask),
                                                       reinterpret_cast<bitmask_type const*>(current_mask)};
                std::vector<size_type> begin_bits{0, 0};
                auto const valid_count = detail::inplace_bitmask_and(
                    device_span<bitmask_type>(current_mask, num_bitmask_words(num_rows)),
                    masks,
                    begin_bits,
                    num_rows,
                    stream);
                auto const new_null_count = num_rows - valid_count;
                input->set_null_count(new_null_count);
            }

            // If the input is also a struct, repeat for all its children. Otherwise just return.
            if (input->type().id() != type_id::STRUCT) {
                return std::move(input);
            }

            auto const current_mask = input->view().null_mask();
            auto const new_null_count = input->null_count(); // this was just computed in the step above
            auto content = input->release();

            // Build new children columns.
            std::for_each(
                content.children.begin(), content.children.end(), [current_mask, stream, mr](auto& child) {
                    child = superimpose_nulls_no_sanitize(
                        current_mask, UNKNOWN_NULL_COUNT, std::move(child), stream, mr);
                });

            // Replace the children columns.
            return make_structs_column(num_rows,
                                             std::move(content.children),
                                             new_null_count,
                                             std::move(*content.null_mask),
                                             stream,
                                             mr);
            */
        }

    } // namespace

    std::unique_ptr<column::column_t> superimpose_nulls(
        std::pmr::memory_resource* resource,
        const bitmask_type* null_mask,
        size_type null_count,
        std::unique_ptr<column::column_t>&& input) {
        input = superimpose_nulls_no_sanitize(resource, null_mask, null_count, std::move(input));
        if (const auto input_view = input->view(); has_nonempty_nulls(input_view)) {
            return purge_nonempty_nulls(resource, input_view);
        }
        return std::move(input);
    }

} // namespace components::dataframe::detail
