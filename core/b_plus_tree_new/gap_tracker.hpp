#pragma once

#include <vector>

namespace core::b_plus_tree {

    class gap_tracker_t {
    public:
        struct range_t {
            size_t offset;
            size_t size;
        };

        gap_tracker_t(size_t min, size_t max) { init(min, max); }

        void init(size_t min, size_t max);

        size_t find_gap(size_t size) noexcept;
        void create_gap(range_t range);
        void clean_gaps() noexcept;

        std::vector<range_t>& empty_spaces() { return empty_spaces_; }
        std::vector<range_t>& untouched_spaces() { return untouched_spaces_; }

    private:
        std::vector<range_t> empty_spaces_;
        std::vector<range_t> untouched_spaces_;
    };

} // namespace core::b_plus_tree