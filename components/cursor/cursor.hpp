#pragma once
#include <components/document/document_view.hpp>
#include <goblin-engineer/core.hpp>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

namespace components::cursor {

    using data_t = components::document::document_view_t;
    using iterator_t = std::vector<data_t>::iterator;
    using const_iterator_t = std::vector<data_t>::const_iterator;

    class data_cursor_t {
    public:
        data_cursor_t() = default;
        data_cursor_t(std::vector<data_t> data);
        std::size_t size() const;
        const_iterator_t begin() const;
        const_iterator_t end() const;
    private:
        std::vector<data_t> data_;
    };

    class sub_cursor_t : public boost::intrusive::list_base_hook<> {
    public:
        sub_cursor_t() = default;
        sub_cursor_t(goblin_engineer::actor_address collection, data_cursor_t* data);
        goblin_engineer::actor_address& address();
        std::size_t size() const;
        const_iterator_t begin() const;
        const_iterator_t end() const;
    private:
        goblin_engineer::actor_address collection_;
        data_cursor_t* data_;
    };

    class cursor_t {
    public:
        cursor_t() = default;
        void push(sub_cursor_t* sub_cursor);
        std::size_t size() const;
        std::vector<std::unique_ptr<sub_cursor_t>>::iterator begin();
        std::vector<std::unique_ptr<sub_cursor_t>>::iterator end();
        const_iterator_t first();
        bool has_next() const;
        const_iterator_t next();
        data_t get() const;
    private:
        std::size_t size_{};
        std::size_t index_sub{};
        const_iterator_t it;
        std::vector<std::unique_ptr<sub_cursor_t>> sub_cursor_;
    };

} // namespace components::cursor
