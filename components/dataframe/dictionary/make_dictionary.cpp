#include "make_dictionary.hpp"

#include <memory>
#include <memory_resource>

#include <dataframe/column/make.hpp>
#include <dataframe/detail/bitmask.hpp>
#include <dataframe/detail/cast.hpp>
#include <dataframe/type_dispatcher.hpp>

namespace components::dataframe::dictionary {

    data_type get_indices_type_for_size(size_type keys_size) {
        if (keys_size <= std::numeric_limits<uint8_t>::max()) {
            return data_type{type_id::uint8};
        }
        if (keys_size <= std::numeric_limits<uint16_t>::max()) {
            return data_type{type_id::uint16};
        }
        return data_type{type_id::uint32};
    }

    namespace {
        struct dispatch_create_indices {
            template<typename IndexType, std::enable_if_t<is_index_type<IndexType>()>* = nullptr>
            std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource* resource,
                                                         column::column_view const& indices) {
                assertion_exception_msg(std::is_unsigned<IndexType>(), "indices must be an unsigned type");
                column::column_view indices_view{indices.type(),
                                                 indices.size(),
                                                 indices.data<IndexType>(),
                                                 nullptr,
                                                 0,
                                                 indices.offset()};
                return std::make_unique<column::column_t>(resource, indices_view);
            }
            template<typename IndexType, std::enable_if_t<!is_index_type<IndexType>()>* = nullptr>
            std::unique_ptr<column::column_t> operator()(std::pmr::memory_resource*, column::column_view const&) {
                assert(false);
            }
        };
    } // namespace

    std::unique_ptr<column::column_t> make_dictionary_column(std::pmr::memory_resource* resource,
                                                             column::column_view const& keys_column,
                                                             column::column_view const& indices_column) {
        assertion_exception_msg(!keys_column.has_nulls(resource), "keys column must not have nulls");
        if (keys_column.is_empty()) {
            return column::make_empty_column(resource, type_id::dictionary32);
        }

        auto keys_copy = std::make_unique<column::column_t>(resource, keys_column);
        auto indices_copy = type_dispatcher(indices_column.type(), dispatch_create_indices{}, resource, indices_column);
        core::buffer null_mask{resource, 0};
        auto null_count = indices_column.null_count(resource);
        if (null_count) {
            null_mask = detail::copy_bitmask(resource, indices_column);
        }

        std::vector<std::unique_ptr<column::column_t>> children;
        children.emplace_back(std::move(indices_copy));
        children.emplace_back(std::move(keys_copy));
        return std::make_unique<column::column_t>(resource,
                                                  data_type{type_id::dictionary32},
                                                  indices_column.size(),
                                                  core::buffer{resource, 0},
                                                  std::move(null_mask),
                                                  null_count,
                                                  std::move(children));
    }

    std::unique_ptr<column::column_t> make_dictionary_column(std::pmr::memory_resource* resource,
                                                             std::unique_ptr<column::column_t> keys_column,
                                                             std::unique_ptr<column::column_t> indices_column,
                                                             core::buffer&& null_mask,
                                                             size_type null_count) {
        assertion_exception_msg(!keys_column->has_nulls(), "keys column must not have nulls");
        assertion_exception_msg(!indices_column->has_nulls(), "indices column must not have nulls");
        assertion_exception_msg(is_unsigned(indices_column->type()), "indices must be type unsigned integer");

        auto count = indices_column->size();
        std::vector<std::unique_ptr<column::column_t>> children;
        children.emplace_back(std::move(indices_column));
        children.emplace_back(std::move(keys_column));
        return std::make_unique<column::column_t>(resource,
                                                  data_type{type_id::dictionary32},
                                                  count,
                                                  core::buffer{resource},
                                                  std::move(null_mask),
                                                  null_count,
                                                  std::move(children));
    }

    namespace {

        struct make_unsigned_fn {
            template<typename T, std::enable_if_t<is_index_type<T>()>* = nullptr>
            constexpr type_id operator()() {
                return type_to_id<std::make_unsigned_t<T>>();
            }
            template<typename T, std::enable_if_t<not is_index_type<T>()>* = nullptr>
            constexpr type_id operator()() {
                return type_to_id<T>();
            }
        };

    } // namespace

    std::unique_ptr<column::column_t> make_dictionary_column(std::pmr::memory_resource* resource,
                                                             std::unique_ptr<column::column_t> keys,
                                                             std::unique_ptr<column::column_t> indices) {
        assertion_exception_msg(!keys->has_nulls(), "keys column must not have nulls");

        // signed integer data can be used directly in the unsigned indices column
        auto const indices_type = type_dispatcher(indices->type(), make_unsigned_fn{});
        auto const indices_size = indices->size();     // these need to be saved
        auto const null_count = indices->null_count(); // before calling release()
        auto contents = indices->release();
        // compute the indices type using the size of the key set
        auto const new_type = get_indices_type_for_size(keys->size());

        // create the dictionary indices: convert to unsigned and remove nulls
        auto indices_column = [&] {
            // If the types match, then just commandeer the column's data buffer.
            if (new_type.id() == indices_type) {
                return std::make_unique<column::column_t>(resource,
                                                          new_type,
                                                          indices_size,
                                                          std::move(*(contents.data.release())),
                                                          core::buffer{resource},
                                                          0);
            }
            // If the new type does not match, then convert the data.
            column::column_view cast_view{data_type{indices_type}, indices_size, contents.data->data()};
            return detail::cast(resource, cast_view, new_type);
        }();

        return make_dictionary_column(resource,
                                      std::move(keys),
                                      std::move(indices_column),
                                      std::move(*(contents.null_mask.release())),
                                      null_count);
    }

} // namespace components::dataframe::dictionary
