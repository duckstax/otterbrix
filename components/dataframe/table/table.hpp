#pragma once

#include <memory>
#include <vector>

#include <dataframe/column/column.hpp>
#include <dataframe/table/table_view.hpp>

namespace components::dataframe::table {

    class table_t {
    public:
        table_t() = default;
        ~table_t() = default;
        table_t(table_t&&) = default;
        table_t& operator=(table_t const&) = delete;
        table_t& operator=(table_t&&) = delete;

        table_t(table_t const& other);
        table_t(std::vector<std::unique_ptr<column::column_t>>&& columns);
        table_t(std::pmr::memory_resource* mr, table_view view);

        [[nodiscard]] size_type num_columns() const noexcept { return _columns.size(); }
        [[nodiscard]] size_type num_rows() const noexcept { return _num_rows; }
        [[nodiscard]] table_view view() const;
        operator table_view() const { return this->view(); };
        mutable_table_view mutable_view();
        operator mutable_table_view() { return this->mutable_view(); };
        std::vector<std::unique_ptr<column::column_t>> release();

        template<typename InputIterator>
        table_view select(InputIterator begin, InputIterator end) const {
            std::vector<column::column_view> columns(std::distance(begin, end));
            std::transform(begin, end, columns.begin(), [this](auto index) { return _columns.at(index)->view(); });
            return table_view(columns);
        }

        [[nodiscard]] table_view select(std::vector<size_type> const& column_indices) const {
            return select(column_indices.begin(), column_indices.end());
        };

        column::column_t& get_column(size_type column_index) { return *(_columns.at(column_index)); }
        [[nodiscard]] column::column_t const& get_column(size_type i) const { return *(_columns.at(i)); }

    private:
        std::pmr::memory_resource* resource_;
        std::vector<std::unique_ptr<column::column_t>> _columns{};
        size_type _num_rows{};
    };

} // namespace components::dataframe::table
