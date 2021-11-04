#include "cursor.hpp"

namespace components::cursor {

    void cursor_t::push(sub_cursor_t* sub_cursor) {
        size_+= sub_cursor->size();
        sub_cursor_.emplace_back(sub_cursor);
    }

    std::size_t cursor_t::size() const {
        return size_;
    }

    auto cursor_t::begin() -> std::list<std::unique_ptr<sub_cursor_t>>::iterator {
        return sub_cursor_.begin();
    }

    auto cursor_t::end() -> std::list<std::unique_ptr<sub_cursor_t>>::iterator {
        return sub_cursor_.end();
    }

    goblin_engineer::actor_address& sub_cursor_t::address() {
        return collection_;
    }

    size_t sub_cursor_t::size() const {
        return data_->size();
    }

    sub_cursor_t::sub_cursor_t(goblin_engineer::actor_address collection, data_cursor_t* data)
        : collection_(collection)
        , data_(data) {
    }
    data_cursor_t::data_cursor_t(std::vector<components::document::document_view_t> data)
        : data_(std::move(data)) {}

    size_t data_cursor_t::size() const {
        return data_.size();
    }
}
