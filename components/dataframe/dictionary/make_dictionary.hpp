#include "dataframe/column/column.hpp"

namespace components::dataframe::dictionary {
    std::unique_ptr<column::column_t> make_dictionary_column(std::pmr::memory_resource*,
                                                             column::column_view const& keys_column,
                                                             column::column_view const& indices_column);

    std::unique_ptr<column::column_t> make_dictionary_column(std::pmr::memory_resource*,
                                                             std::unique_ptr<column::column_t> keys_column,
                                                             std::unique_ptr<column::column_t> indices_column,
                                                             core::buffer&& null_mask,
                                                             size_type null_count);

    std::unique_ptr<column::column_t> make_dictionary_column(std::pmr::memory_resource*,
                                                             std::unique_ptr<column::column_t> keys,
                                                             std::unique_ptr<column::column_t> indices);

} // namespace components::dataframe::dictionary