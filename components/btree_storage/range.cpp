#include "range.hpp"

namespace components::btree {

    void data_ranges_t::add_range(const range_t& range) {
        range_t new_range = range;
        for (auto it = ranges_.begin(); it != ranges_.end();) {
            if (is_cross_(new_range, *it)) {
                auto it_cross = it;
                ++it;
                new_range = cross_(new_range, *it_cross);
                ranges_.erase(it_cross);
            } else {
                ++it;
            }
        }
        ranges_.push_back(new_range);
    }

    void data_ranges_t::clear() {
        ranges_.clear();
    }

    bool data_ranges_t::empty() const {
        return ranges_.empty();
    }

    void data_ranges_t::sort() {
        ranges_.sort([](const range_t& r1, const range_t& r2) {
            return r1.first < r2.first;
        });
    }

    void data_ranges_t::reverse_sort() {
        ranges_.sort([](const range_t& r1, const range_t& r2) {
            return r1.first > r2.first;
        });
    }

    const data_ranges_t::ranges_t& data_ranges_t::ranges() const {
        return ranges_;
    }

    bool data_ranges_t::is_cross_(const range_t& r1, const range_t& r2) {
        return r1.first == r2.second + 1 || r2.first == r1.second + 1;
    }

    data_ranges_t::range_t data_ranges_t::cross_(const range_t& r1, const range_t& r2) {
        return {std::min(r1.first, r2.first), std::max(r1.second, r2.second)};
    }

} //namespace components::document