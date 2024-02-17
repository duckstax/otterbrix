#include <dataframe/table/table.hpp>
#include <dataframe/table/table_view.hpp>
#include <memory>

namespace components::dataframe::table {

    table_t::table_t(table_t const& other)
        : resource_(other.resource_)
        , _num_rows{other.num_rows()} {
        _columns.reserve(other._columns.size());
        for (auto const& c : other._columns) {
            _columns.emplace_back(std::make_unique<column::column_t>(resource_, *c));
        }
    }

    table_t::table_t(std::vector<std::unique_ptr<column::column_t>>&& columns)
        : _columns{std::move(columns)} {
        if (num_columns() > 0) {
            for (auto const& c : _columns) {
                assertion_exception_msg(c, "Unexpected null column");
                assertion_exception_msg(c->size() == _columns.front()->size(),
                                        "Column size mismatch: " + std::to_string(c->size()) +
                                            " != " + std::to_string(_columns.front()->size()));
            }
            _num_rows = _columns.front()->size();
        } else {
            _num_rows = 0;
        }
    }

    table_t::table_t(std::pmr::memory_resource* mr, table_view view)
        : resource_(mr)
        , _num_rows{view.num_rows()} {
        _columns.reserve(view.num_columns());
        for (auto const& c : view) {
            _columns.emplace_back(std::make_unique<column::column_t>(mr, c));
        }
    }

    table_view table_t::view() const {
        std::vector<column::column_view> views;
        views.reserve(_columns.size());
        for (auto const& c : _columns) {
            views.push_back(c->view());
        }
        return table_view{views};
    }

    mutable_table_view table_t::mutable_view() {
        std::vector<column::mutable_column_view> views;
        views.reserve(_columns.size());
        for (auto const& c : _columns) {
            views.push_back(c->mutable_view());
        }
        return mutable_table_view{views};
    }

    // Release ownership of columns
    std::vector<std::unique_ptr<column::column_t>> table_t::release() {
        _num_rows = 0;
        return std::move(_columns);
    }

} // namespace components::dataframe::table
