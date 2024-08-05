#include "block.hpp"
#include "gap_tracker.hpp"
#include <algorithm>
#include <cassert>
#include <crc32c/crc32c.h>
#include <cstring>

namespace core::b_plus_tree {
    size_t SECTOR_SIZE = 4096;

    block_t::iterator::iterator(const block_t* block, block_t::metadata* metadata)
        : block_(block)
        , metadata_(metadata) {
        rebuild_data();
    }
    void block_t::iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_metadata_) {
            data_.data = block_->internal_buffer_ + metadata_->offset;
            data_.size = metadata_->size;
        } else {
            data_.data = nullptr;
            data_.size = 0;
        }
    }

    block_t::r_iterator::r_iterator(const block_t* block, block_t::metadata* metadata)
        : block_(block)
        , metadata_(metadata) {
        rebuild_data();
    }
    void block_t::r_iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_metadata_) {
            data_.data = block_->internal_buffer_ + metadata_->offset;
            data_.size = metadata_->size;
        } else {
            data_.data = nullptr;
            data_.size = 0;
        }
    }

    block_t::block_t(std::pmr::memory_resource* resource, index_t (*func)(const item_data&))
        : resource_(resource)
        , key_func_(func) {}

    block_t::~block_t() {
        if (!internal_buffer_ || !is_valid_) {
            return;
        }
        resource_->deallocate(internal_buffer_, full_size_);
    }

    void block_t::initialize(size_t size) {
        assert(!is_valid_ && "block was already initialized!");
        assert(size < MAX_BLOCK_SIZE && "block cannot handle this size!");
        assert((size & (SECTOR_SIZE - 1)) == 0 && "block size should be a multiple of SECTOR_SIZE!");
        internal_buffer_ = static_cast<data_ptr_t>(resource_->allocate(size));
        full_size_ = size;
        checksum_ = reinterpret_cast<size_t*>(internal_buffer_);
        count_ = reinterpret_cast<uint32_t*>(checksum_ + 1);
        *count_ = 0;
        unique_indices_count_ = count_ + 1;
        *unique_indices_count_ = 0;
        end_ = reinterpret_cast<metadata*>(internal_buffer_ + size);

        restore_block();
    }

    size_t block_t::available_memory() const { return available_memory_; }

    size_t block_t::occupied_memory() const { return full_size_ - available_memory_ - header_size; }

    bool block_t::is_memory_available(size_t request_size) const {
        return request_size + metadata_size <= available_memory_;
    }

    bool block_t::is_empty() const { return last_metadata_ == end_; }

    bool block_t::is_valid() const { return is_valid_; }

    bool block_t::append(data_ptr_t data, size_t size) noexcept { return append({data, size}); }

    bool block_t::append(item_data item) noexcept {
        assert(is_valid_ && "block is not initialized!");
        assert(item.size != 0 && "can not append an empty buffer!");
        if (!is_memory_available(item.size)) {
            return false;
        }

        index_t index = key_func_(item);
        auto range = find_index_range_(index);
        for (metadata* it = range.begin; it != range.end; it++) {
            if (it->size == item.size && std::memcmp(item.data, internal_buffer_ + it->offset, item.size) == 0) {
                return false;
            }
        }

        (*count_)++;
        if (range.begin == range.end) {
            (*unique_indices_count_)++;
        }

        if (range.begin != range.end) {
            bool r = true;
            assert(r);
        }

        std::memmove(last_metadata_ - 1, last_metadata_, (range.begin - last_metadata_) * metadata_size);
        range.begin--;
        last_metadata_--;
        range.begin->offset = static_cast<uint32_t>(buffer_ - internal_buffer_);
        range.begin->size = item.size;

        std::memcpy(buffer_, item.data, item.size);
        buffer_ += item.size;
        available_memory_ -= item.size + metadata_size;

        return true;
    }
    bool block_t::remove(data_ptr_t data, size_t size) noexcept { return remove({data, size}); }

    bool block_t::remove(item_data item) noexcept {
        assert(is_valid_ && "block is not initialized!");

        index_t index = key_func_(item);
        auto range = find_index_range_(index);
        if (range.begin == range.end) {
            return false;
        }

        (*count_)--;
        if (range.begin + 1 == range.end) {
            (*unique_indices_count_)--;
        }
        for (metadata* it = range.begin; it != range.end; it++) {
            if (it->size == item.size && std::memcmp(item.data, internal_buffer_ + it->offset, item.size) == 0) {
                uint32_t chunk_size = it->size;
                data_ptr_t chunk_start = internal_buffer_ + it->offset;
                data_ptr_t next_chunk_start = chunk_start + it->size;
                std::memmove(chunk_start, next_chunk_start, buffer_ - next_chunk_start);
                buffer_ -= it->size;
                available_memory_ += it->size + metadata_size;
                std::memmove(last_metadata_ + 1, last_metadata_, (it - last_metadata_) * metadata_size);
                last_metadata_++;

                uint32_t offset = chunk_start - internal_buffer_;
                for (auto it_ = last_metadata_; it_ != end_; it_++) {
                    if (it_->offset > offset) {
                        it_->offset -= chunk_size;
                    }
                }
            }
        }

        return true;
    }

    bool block_t::remove_index(const index_t& index) {
        assert(is_valid_ && "block is not initialized!");

        auto range = find_index_range_(index);
        if (range.begin == range.end) {
            return false;
        }

        size_t batch_size = range.end - range.begin;

        *count_ -= batch_size;
        (*unique_indices_count_)--;

        gap_tracker_t tracker(header_size, data_ptr_t(buffer_) - internal_buffer_);

        for (metadata* it = range.begin; it != range.end; it++) {
            tracker.create_gap(gap_tracker_t::range_t{it->offset, it->size});
        }

        tracker.clean_gaps();
        std::memmove(last_metadata_ + batch_size, last_metadata_, (range.begin - last_metadata_) * metadata_size);
        available_memory_ += batch_size * metadata_size;
        last_metadata_ += batch_size;

        assert(tracker.untouched_spaces().size() != 0 && "gap tracker is broken, should not be empty");
        uint32_t tracked_offset = 0;
        uint32_t required_offset = 0;
        auto next_it = tracker.untouched_spaces().begin() + 1;
        for (auto it = tracker.untouched_spaces().begin(); next_it != tracker.untouched_spaces().end();
             ++it, ++next_it) {
            tracked_offset = it->offset + it->size - required_offset;
            required_offset += next_it->offset - (it->offset + it->size);
            std::memmove(internal_buffer_ + tracked_offset, internal_buffer_ + next_it->offset, next_it->size);
            for (metadata* meta = last_metadata_; meta != end_; meta++) {
                if (meta->offset >= next_it->offset && meta->offset < next_it->offset + next_it->size) {
                    meta->offset -= required_offset;
                }
            }
        }
        buffer_ -= required_offset;

        return true;
    }

    bool block_t::contains_index(const index_t& index) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);
        return range.begin != range.end;
    }

    bool block_t::contains(item_data item) const {
        assert(is_valid_ && "block is not initialized!");

        index_t index = key_func_(item);
        auto range = find_index_range_(index);

        for (metadata* it = range.begin; it != range.end; it++) {
            if (it->size == item.size && std::memcmp(item.data, internal_buffer_ + it->offset, item.size) == 0) {
                return true;
            }
        }

        return false;
    }

    size_t block_t::item_count(const index_t& index) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);

        return range.end - range.begin;
    }

    block_t::item_data block_t::get_item(const index_t& index, size_t position) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);

        assert(range.end - range.begin > position && "index position is out of range");

        return {internal_buffer_ + (range.begin + position)->offset, (range.begin + position)->size};
    }

    void block_t::get_items(std::vector<item_data>& items, const index_t& index) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);

        items.reserve(items.size() + range.end - range.begin);
        for (metadata* it = range.begin; it != range.end; it++) {
            items.emplace_back(internal_buffer_ + it->offset, it->size);
        }
    }

    size_t block_t::count() const {
        assert(is_valid_ && "block is not initialized!");
        return *count_;
    }

    size_t block_t::unique_indices_count() const {
        assert(is_valid_ && "block is not initialized!");
        return *unique_indices_count_;
    }

    block_t::index_t block_t::min_index() const {
        assert(is_valid_ && "block is not initialized!");
        if (last_metadata_ == end_) {
            return index_t();
        }

        item_data item(internal_buffer_ + (end_ - 1)->offset, (end_ - 1)->size);
        return key_func_(item);
    }
    block_t::index_t block_t::max_index() const {
        assert(is_valid_ && "block is not initialized!");
        if (last_metadata_ == end_) {
            return index_t();
        }

        item_data item(internal_buffer_ + last_metadata_->offset, last_metadata_->size);
        return key_func_(item);
    }

    data_ptr_t block_t::internal_buffer() {
        assert(is_valid_ && "block is not initialized!");
        return internal_buffer_;
    }

    size_t block_t::block_size() const { return full_size_; }

    void block_t::reset() {
        *count_ = 0;
        last_metadata_ = end_;
        restore_block();
    }

    void block_t::restore_block() {
        last_metadata_ = end_ - *count_;
        buffer_ = internal_buffer_ + header_size;
        for (auto it = last_metadata_; it < end_; it++) {
            buffer_ += it->size;
        }
        available_memory_ = reinterpret_cast<data_ptr_t>(last_metadata_) - buffer_;
        is_valid_ = true;
    }

    void block_t::resize(size_t new_size) {
        if (!is_valid_) {
            return;
        }

        assert((new_size & (SECTOR_SIZE - 1)) == 0 && "block size should be a multiple of SECTOR_SIZE!");
        assert(occupied_memory() < new_size && "block data won't fit in new size");
        data_ptr_t new_buffer = static_cast<data_ptr_t>(resource_->allocate(new_size));
        std::memcpy(new_buffer, internal_buffer_, buffer_ - internal_buffer_);
        std::memcpy(new_buffer + new_size - *unique_indices_count_ * metadata_size,
                    last_metadata_,
                    *unique_indices_count_ * metadata_size);

        size_t buffer_offset = buffer_ - internal_buffer_;
        resource_->deallocate(internal_buffer_, full_size_);
        internal_buffer_ = new_buffer;
        buffer_ = internal_buffer_ + buffer_offset;
        available_memory_ = new_size - full_size_;
        full_size_ = new_size;
        checksum_ = reinterpret_cast<uint64_t*>(internal_buffer_);
        count_ = reinterpret_cast<uint32_t*>(checksum_ + 1);
        unique_indices_count_ = count_ + 1;
        end_ = reinterpret_cast<metadata*>(internal_buffer_ + new_size);
        last_metadata_ = end_ - *unique_indices_count_;
    }

    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> block_t::split_append(const index_t& index,
                                                                                        item_data item) {}

    std::unique_ptr<block_t> block_t::split(size_t count) {
        assert(is_valid_ && "block is not initialized!");
        assert(count <= *count_);
    }

    void block_t::merge(std::unique_ptr<block_t> other) {
        assert(is_valid_ && "other is not initialized!");
        assert(other->is_valid_ && "other block is not initialized!");
        assert(available_memory_ >= other->occupied_memory());
        assert(available_memory_ + (buffer_ - internal_buffer_) <= full_size_);
        if (other->count() == 0) {
            return;
        }
    }

    void block_t::recalculate_checksum() { *checksum_ = calculate_checksum_(); }

    bool block_t::varify_checksum() const { return *checksum_ == calculate_checksum_(); }

    block_t::index_t block_t::find_index(data_ptr_t data, size_t size) const noexcept {
        return key_func_({data, size});
    }

    block_t::metadata_range block_t::find_index_range_(const index_t& index) const {
        if (!is_valid_) {
            return {end_, end_};
        }

        metadata_range result;
        result.begin =
            std::lower_bound(last_metadata_, end_, index, [this](const metadata& meta, const index_t& index) {
                return key_func_({internal_buffer_ + meta.offset, meta.size}) > index;
            });
        result.end = std::lower_bound(result.begin, end_, index, [this](const metadata& meta, const index_t& index) {
            return key_func_({internal_buffer_ + meta.offset, meta.size}) >= index;
        });
        return result;
    }

    size_t block_t::calculate_checksum_() const {
        assert(is_valid_ && "block is not initialized!");
        data_ptr_t crc_buffer = reinterpret_cast<data_ptr_t>(count_);
        size_t size = full_size_ - (crc_buffer - internal_buffer_);
        return crc32c::Crc32c(crc_buffer, size);
    }

} // namespace core::b_plus_tree
