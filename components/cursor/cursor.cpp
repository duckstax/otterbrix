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
        return static_cast<std::size_t>(current_index_ + 1) < size_;
    }

    const data_t *cursor_t::next() {
        return get(static_cast<std::size_t>(++current_index_));
    }

    const data_t *cursor_t::get() const {
        return get(static_cast<std::size_t>(current_index_ < 0 ? 0 : current_index_));
    }

    const data_t *cursor_t::get(std::size_t index) const {
        return sorted_.empty()
                ? get_unsorted(index)
                : get_sorted(index);
    }

    void cursor_t::sort(std::function<bool(data_t*, data_t*)> sorter) {
        create_list_by_sort();
        sorted_.sort(sorter);
        current_index_ = start_index;
    }

    void cursor_t::create_list_by_sort() {
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
            return *(std::next(sorted_.begin(), static_cast<int32_t>(index)));
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

    std::vector<data_t> &data_cursor_t::data() {
        return data_;
    }

}
