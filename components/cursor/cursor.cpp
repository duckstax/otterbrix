#include "cursor.hpp"
#include <boost/json.hpp>
namespace components::cursor {

    void cursor_t::push(sub_cursor_t* sub_cursor) {
        size_ += sub_cursor->size();
        sub_cursor_.emplace_back(sub_cursor);
    }

    std::size_t cursor_t::size() const {
        return size_;
    }

    std::pmr::vector<std::unique_ptr<sub_cursor_t>>::iterator cursor_t::begin() {
        return sub_cursor_.begin();
    }

    std::pmr::vector<std::unique_ptr<sub_cursor_t>>::iterator cursor_t::end() {
        return sub_cursor_.end();
    }

    bool cursor_t::has_next() const {
        return static_cast<std::size_t>(current_index_ + 1) < size_;
    }

    data_ptr cursor_t::next() {
        return get(static_cast<std::size_t>(++current_index_));
    }

    data_ptr cursor_t::get() const {
        return get(static_cast<std::size_t>(current_index_ < 0 ? 0 : current_index_));
    }

    data_ptr cursor_t::get(std::size_t index) const {
        return sorted_.empty()
                   ? get_unsorted(index)
                   : get_sorted(index);
    }

    void cursor_t::sort(std::function<bool(data_ptr, data_ptr)> sorter) {
        create_list_by_sort();
        std::sort(sorted_.begin(), sorted_.end(), std::move(sorter));
        current_index_ = start_index;
    }

    void cursor_t::create_list_by_sort() {
        if (sorted_.empty()) {
            sorted_.reserve(size_);
            for (auto& sub : sub_cursor_) {
                for (auto& document : sub->data()) {
                    sorted_.emplace_back(&document);
                }
            }
        }
    }

    data_ptr cursor_t::get_sorted(std::size_t index) const {
        if (index < size_) {
            return sorted_.at(index);
        }
        return nullptr;
    }

    data_ptr cursor_t::get_unsorted(std::size_t index) const {
        if (index < size_) {
            auto i = index;
            for (const auto& sub : sub_cursor_) {
                if (i < sub->size()) {
                    return &sub->data()[i];
                }
                i -= sub->size();
            }
        }
        return nullptr;
    }

    cursor_t::cursor_t(std::pmr::memory_resource* resource)
        : sub_cursor_(resource)
        , sorted_(resource) {}

    actor_zeta::address_t& sub_cursor_t::address() {
        return collection_;
    }

    size_t sub_cursor_t::size() const {
        return data_.size();
    }

    std::pmr::vector<data_t>& sub_cursor_t::data() {
        return data_;
    }

    sub_cursor_t::sub_cursor_t(std::pmr::memory_resource* resource, actor_zeta::address_t collection)
        : collection_(collection)
        , data_(resource) {
    }

    void sub_cursor_t::append(data_t data) {
        data_.push_back(data);
    }

} // namespace components::cursor
