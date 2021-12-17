#pragma once
#include <components/document/document_view.hpp>
#include <goblin-engineer/core.hpp>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

namespace components::cursor {

    using data_t = components::document::document_view_t;
    using data_ptr = const data_t*;
    using index_t = int32_t;
    constexpr index_t start_index = -1;

    class sub_cursor_t : public boost::intrusive::list_base_hook<> {
    public:
        sub_cursor_t(goblin_engineer::address_t collection, const std::vector<data_t> &data);
        goblin_engineer::address_t& address();
        std::size_t size() const;
        std::vector<data_t> &data();
    private:
        goblin_engineer::address_t collection_;
        std::vector<data_t> data_;
    };

    class cursor_t {
    public:
        cursor_t() = default;
        void push(sub_cursor_t* sub_cursor);
        std::size_t size() const;
        std::vector<std::unique_ptr<sub_cursor_t>>::iterator begin();
        std::vector<std::unique_ptr<sub_cursor_t>>::iterator end();
        bool has_next() const;
        data_ptr next();
        data_ptr get() const;
        data_ptr get(std::size_t index) const;
        void sort(std::function<bool(data_ptr, data_ptr)> sorter);
    private:
        std::size_t size_{};
        index_t current_index_{start_index};
        std::vector<std::unique_ptr<sub_cursor_t>> sub_cursor_;
        std::list<data_ptr> sorted_;

        void create_list_by_sort();
        data_ptr get_sorted(std::size_t index) const;
        data_ptr get_unsorted(std::size_t index) const;
    };

} // namespace components::cursor
