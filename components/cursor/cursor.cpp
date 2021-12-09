#include "cursor.hpp"

namespace components::cursor {

    void cursor_t::push(sub_cursor_t* sub_cursor) {
        size_+= sub_cursor->size();
        sub_cursor_.emplace_back(sub_cursor);
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

    bool cursor_t::has_next() const {
        return size_ > 0 && (current_index_ < static_cast<index_t>(sub_cursor_.size()) - 1 || sub_cursor_.at(static_cast<std::size_t>(current_index_))->has_next());
    }

    bool cursor_t::next() {
        if (current_index_ < 0) current_index_ = 0;
        current_ = nullptr;
        while (!current_ && current_index_ < static_cast<index_t>(sub_cursor_.size())) {
            current_ = sub_cursor_.at(static_cast<std::size_t>(current_index_))->next();
            if (!current_) current_index_++;
        }
        return current_ != nullptr;
    }

    const data_t *cursor_t::get() const {
        return current_;
    }

    const data_t *cursor_t::get(std::size_t index) const {
        return sorted_.empty()
                ? get_unsorted(index)
                : get_sorted(index);
    }

    void cursor_t::sort(std::function<bool(data_t*, data_t*)> sorter) {
        createListBySort();
        sorted_.sort(sorter);
    }

    void cursor_t::createListBySort() {
        if (sorted_.empty()) {
            for (auto &sub : sub_cursor_) {
                for (auto &document : sub->data()) {
                    sorted_.emplace_back(&document);
                }
            }
        }
    }

    const data_t *cursor_t::get_sorted(std::size_t index) const {
        if (index < size_) {
            return *(std::next(sorted_.begin(), static_cast<long>(index)));
        }
        return nullptr;
    }

    const data_t *cursor_t::get_unsorted(std::size_t index) const {
        if (index < size_) {
            auto i = index;
            for (const auto &sub : sub_cursor_) {
                if (i < sub->size()) {
                    return &sub->data()[i];
                }
                i -= sub->size();
            }
        }
        return nullptr;
    }

    goblin_engineer::address_t& sub_cursor_t::address() {
        return collection_;
    }

    size_t sub_cursor_t::size() const {
        return data_->size();
    }

    bool sub_cursor_t::has_next() const {
        return current_index_ < static_cast<index_t>(size()) - 1;
    }

    const data_t *sub_cursor_t::next() {
        current_index_++;
        if (current_index_ < static_cast<index_t>(size())) {
            return data_->get(static_cast<std::size_t>(current_index_));
        }
        return nullptr;
    }

    std::vector<data_t> &sub_cursor_t::data() {
        return data_->data();
    }

    sub_cursor_t::sub_cursor_t(goblin_engineer::address_t collection, data_cursor_t* data)
        : collection_(collection)
        , data_(data) {
    }
    data_cursor_t::data_cursor_t(std::vector<data_t> data)
        : data_(std::move(data)) {}

    size_t data_cursor_t::size() const {
        return data_.size();
    }

    const data_t *data_cursor_t::get(std::size_t index) const {
        return &data_.at(index);
    }

    std::vector<data_t> &data_cursor_t::data() {
        return data_;
    }

}
