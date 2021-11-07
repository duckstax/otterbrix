#include "cursor.hpp"

namespace components::cursor {

    void cursor_t::push(sub_cursor_t* sub_cursor) {
        size_+= sub_cursor->size();
        sub_cursor_.emplace_back(sub_cursor);
        if (sub_cursor_.size() == 1) first();
    }

    std::size_t cursor_t::size() const {
        return size_;
    }

    std::vector<std::unique_ptr<sub_cursor_t>>::iterator cursor_t::begin() {
        return sub_cursor_.begin();
    }

    std::vector<std::unique_ptr<sub_cursor_t>>::iterator cursor_t::end() {
        return sub_cursor_.end();
    }

    const_iterator_t cursor_t::first() {
        index_sub = 0;
        it = begin()->get()->begin();
        return it;
    }

    bool cursor_t::has_next() const {
        return !sub_cursor_.empty() && (index_sub < sub_cursor_.size() - 1 || it + 1 != sub_cursor_.end()->get()->end());
    }

    const_iterator_t cursor_t::next() {
        ++it;
        if (it == sub_cursor_[index_sub]->end()) {
            ++index_sub;
            if (index_sub < sub_cursor_.size()) {
                it = sub_cursor_[index_sub]->begin();
            }
        }
        return it;
    }

    data_t cursor_t::get() const {
        return *it;
    }

    goblin_engineer::actor_address& sub_cursor_t::address() {
        return collection_;
    }

    size_t sub_cursor_t::size() const {
        return data_->size();
    }

    const_iterator_t sub_cursor_t::begin() const {
        return data_->begin();
    }

    const_iterator_t sub_cursor_t::end() const {
        return data_->end();
    }

    sub_cursor_t::sub_cursor_t(goblin_engineer::actor_address collection, data_cursor_t* data)
        : collection_(collection)
        , data_(data) {
    }
    data_cursor_t::data_cursor_t(std::vector<data_t> data)
        : data_(std::move(data)) {}

    size_t data_cursor_t::size() const {
        return data_.size();
    }

    const_iterator_t data_cursor_t::begin() const {
        return data_.begin();
    }

    const_iterator_t data_cursor_t::end() const {
        return data_.end();
    }
}
