#include "gap_tracker.hpp"

#include <cassert>

namespace core::b_plus_tree {

    void gap_tracker_t::init(size_t min, size_t max) {
        untouched_spaces_.clear();
        untouched_spaces_.push_back({min, max - min});
        empty_spaces_.clear();
    }

    //size_t gap_tracker_t::find_gap(size_t size) noexcept {
    //    auto untuched_gap = empty_spaces_.begin();
    //    for (; untuched_gap < empty_spaces_.end(); untuched_gap++) {
    //        if (untuched_gap->size >= size) {
    //            size_t result = untuched_gap->offset;
    //            if (untuched_gap->size == size) {
    //                empty_spaces_.erase(untuched_gap);
    //            } else {
    //                untuched_gap->offset += size;
    //                untuched_gap->size -= size;
    //            }
    //            return result;
    //        }
    //    }
    //    assert(false && "Not enough memory in gap_tracker_t");
    //    return empty_spaces_.back().offset + empty_spaces_.back().size;
    //}

    void gap_tracker_t::create_gap(range_t range) {
        auto gap = empty_spaces_.begin();
        for (; gap < empty_spaces_.end(); ++gap) {
            if (gap->offset > range.offset) {
                break;
            }
        }
        empty_spaces_.insert(gap, range);
        auto untuched_gap = untouched_spaces_.begin();
        for (; untuched_gap < untouched_spaces_.end(); ++untuched_gap) {
            if (untuched_gap->offset > range.offset) {
                break;
            }
        }
        assert(untuched_gap != untouched_spaces_.begin() && "required untuched_gap is out of tracker scope");
        auto prev = untuched_gap - 1;
        range_t next;
        next.offset = range.offset + range.size;
        next.size = prev->size + prev->offset - range.offset - range.size;
        prev->size = range.offset - prev->offset;
        if (next.size != 0) {
            untouched_spaces_.emplace(untuched_gap, std::move(next));
        }
    }
    size_t gap_tracker_t::find_gap(size_t size) noexcept {}

    void gap_tracker_t::clean_gaps() noexcept {
        for (size_t i = 0; i < empty_spaces_.size() - 1;) {
            if (empty_spaces_[i].offset + empty_spaces_[i].size == empty_spaces_[i + 1].offset) {
                empty_spaces_[i].size += empty_spaces_[i + 1].size;
                empty_spaces_.erase(empty_spaces_.begin() + i + 1);
            } else {
                i++;
            }
        }
        for (size_t i = 0; i < untouched_spaces_.size() - 1;) {
            if (untouched_spaces_[i].offset + untouched_spaces_[i].size == untouched_spaces_[i + 1].offset) {
                untouched_spaces_[i].size += untouched_spaces_[i + 1].size;
                untouched_spaces_.erase(untouched_spaces_.begin() + i + 1);
            } else {
                i++;
            }
        }
    }

} // namespace core::b_plus_tree