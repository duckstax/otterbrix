#pragma once
#include <components/document/document_view.hpp>
#include <goblin-engineer/core.hpp>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

namespace components::cursor {

    using data_t = components::document::document_view_t;
    using index_t = int32_t;

    class data_cursor_t {
    public:
        data_cursor_t() = default;
        data_cursor_t(std::vector<data_t> data);
        std::size_t size() const;
        const data_t *get(std::size_t index) const;
        std::vector<data_t> &data();
    private:
        std::vector<data_t> data_;
    };

    class sub_cursor_t : public boost::intrusive::list_base_hook<> {
    public:
        sub_cursor_t() = default;
        sub_cursor_t(goblin_engineer::address_t collection, data_cursor_t* data);
        goblin_engineer::address_t& address();
        std::size_t size() const;
        bool has_next() const;
        const data_t *next();
        std::vector<data_t> &data();
    private:
        goblin_engineer::address_t collection_;
        data_cursor_t* data_;
        index_t current_index_{-1};
    };

    class cursor_t {
    public:
        cursor_t() = default;
        void push(sub_cursor_t* sub_cursor);
        std::size_t size() const;
        std::vector<std::unique_ptr<sub_cursor_t>>::iterator begin();
        std::vector<std::unique_ptr<sub_cursor_t>>::iterator end();
        bool has_next() const;
        bool next();
        const data_t *get() const;
        const data_t *get(std::size_t index) const;
        void sort(std::function<bool(data_t*, data_t*)> sorter);
    private:
        std::size_t size_{};
        index_t current_index_{-1};
        const data_t *current_{nullptr};
        std::vector<std::unique_ptr<sub_cursor_t>> sub_cursor_;
        std::list<data_t*> sorted_;

        void createListBySort();
        const data_t *get_sorted(std::size_t index) const;
        const data_t *get_unsorted(std::size_t index) const;
    };

} // namespace components::cursor
