#include "block.hpp"
#include <cassert>
#include <cstring>
#include <algorithm>

namespace core::b_plus_tree
{
    size_t SECTOR_SIZE = 4096;

    block_t::iterator::iterator(const block_t* block, block_t::item_metadata* metadata) : block_(block), metadata_(metadata) {
        rebuild_data();
    }
    void block_t::iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_item_metadata_) {
            data_.id = metadata_->id;
            data_.data = block_->internal_buffer_ + metadata_->offset;
            data_.size = metadata_->size;
        } else {
            data_.id = INVALID_ID;
            data_.data = nullptr;
            data_.size = 0;
        }
    }

    block_t::r_iterator::r_iterator(const block_t* block, block_t::item_metadata* metadata) : block_(block), metadata_(metadata) {
        rebuild_data();
    }
    void block_t::r_iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_item_metadata_) {
            data_.id = metadata_->id;
            data_.data = block_->internal_buffer_ + metadata_->offset;
            data_.size = metadata_->size;
        } else {
            data_.id = INVALID_ID;
            data_.data = nullptr;
            data_.size = 0;
        }
    }

    block_t::block_t(std::pmr::memory_resource* resource) : resource_(resource) { }

    block_t::~block_t() {
        if (!internal_buffer_ || !is_valid_) {
            return;
        }
        resource_->deallocate(internal_buffer_, full_size_);
    }

    void block_t::initialize(size_t size) {
        assert(!is_valid_ && "block was already initialized!");
        assert((size & (SECTOR_SIZE - 1)) == 0 && "block size should be a multiple of SECTOR_SIZE!");
        internal_buffer_ = static_cast<data_ptr_t>(resource_->allocate(size));
        full_size_ = size;
        checksum_ = reinterpret_cast<uint64_t*>(internal_buffer_);
        count_ = reinterpret_cast<size_t*>(checksum_ + 1);
        *count_ = 0;
        end_ = reinterpret_cast<item_metadata*>(internal_buffer_ + size);

        restore_block();
    }

    size_t block_t::available_memory() const { return available_memory_; }

    size_t block_t::occupied_memory() const { return full_size_ - available_memory_ - header_size; }

    bool block_t::is_memory_available(size_t request_size) const {
        return request_size <= available_memory_ + item_metadata_size;
    }

    bool block_t::is_empty() const { return last_item_metadata_ == end_; }
    
    bool block_t::is_valid() const { return is_valid_; }

    bool block_t::append(uint64_t id, const_data_ptr_t append_buffer, size_t buffer_size) {
        assert(is_valid_ && "block is not initialized!");
        if (!is_memory_available(buffer_size)) {
            return false;
        }

        auto item = find_item_(id);
        if (item != end_ && item->id == id) {
            return false;
        }

        std::memmove(last_item_metadata_ - 1, last_item_metadata_, (item - last_item_metadata_) * item_metadata_size);
        item--;
        last_item_metadata_--;
        item->id = id;
        item->offset = buffer_ - internal_buffer_;
        item->size = buffer_size;

        std::memcpy(buffer_, append_buffer, buffer_size);
        buffer_ += buffer_size;
        available_memory_ -= buffer_size + item_metadata_size;

        (*count_)++;
        return true;
    }

    bool block_t::remove(uint64_t id) {
        assert(is_valid_ && "block is not initialized!");
        auto item = find_item_(id);

        if (item == end_ || item->id != id) {
            return false;
        }

        size_t chunk_size = item->size;
        if (chunk_size == 0) {
            std::memmove(last_item_metadata_ + 1, last_item_metadata_, (item - last_item_metadata_) * item_metadata_size);
            last_item_metadata_++;
            available_memory_ += item_metadata_size;
        } else {
            data_ptr_t chunk_start = internal_buffer_ + item->offset;
            data_ptr_t next_chunk_start = chunk_start + (item)->size;
            std::memmove(chunk_start, next_chunk_start, buffer_ - next_chunk_start);
            buffer_ -= item->size;
            available_memory_ += item->size + item_metadata_size;
            std::memmove(last_item_metadata_ + 1, last_item_metadata_, (item - last_item_metadata_) * item_metadata_size);
            last_item_metadata_++;

            size_t offset = chunk_start - internal_buffer_;
            for(auto it = last_item_metadata_; it != end_; it++) {
                if (it->offset > offset) {
                    it->offset -= chunk_size;
                }
            }
        }

        (*count_)--;
        return true;
    }

    bool block_t::contains(uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto item = find_item_(id);
        return (item != end_ && item->id == id) ? true : false;
    }

    size_t block_t::size_of(uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto item = find_item_(id);

        if (item == end_ || item->id != id) {
            return 0;
        } else {
            return item->size;
        }
    }

    data_ptr_t block_t::data_of(uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto item = find_item_(id);

        if (item == end_ || item->id != id) {
            return nullptr;
        } else {
            return internal_buffer_ + item->offset;
        }
    }
    
    std::pair<data_ptr_t, size_t> block_t::get_item(uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto item = find_item_(id);

        if (item == end_ || item->id != id) {
            return {nullptr, 0};
        } else {
            return {internal_buffer_ + item->offset, item->size};
        }
    }

    size_t block_t::count() const { assert(is_valid_ && "block is not initialized!"); return *count_; }

    uint64_t block_t::first_id() const { assert(is_valid_ && "block is not initialized!"); return last_item_metadata_ == end_ ? INVALID_ID : (end_ - 1)->id; }
    uint64_t block_t::last_id() const { assert(is_valid_ && "block is not initialized!"); return last_item_metadata_ == end_ ? INVALID_ID : last_item_metadata_->id; }

    data_ptr_t block_t::internal_buffer() { assert(is_valid_ && "block is not initialized!"); return internal_buffer_; }

    size_t block_t::block_size() const { return full_size_; }

    void block_t::reset() {
        *count_ = 0;
        last_item_metadata_ = end_;
        restore_block();
    }

    void block_t::restore_block() {
        last_item_metadata_ = end_ - *count_;
        buffer_ = internal_buffer_ + header_size;
        for(auto it = last_item_metadata_; it < end_; it++) {
            buffer_ += it->size;
        }
        available_memory_ = reinterpret_cast<data_ptr_t>(last_item_metadata_) - buffer_;
        is_valid_ = true;
    }

    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> block_t::split_append(uint64_t id, const_data_ptr_t append_buffer, size_t buffer_size) {
        // if append is possible, split in half, append to required part
        // if append is not possible, split by required id, try again
        // if it fails again, third block is required
        assert(is_valid_ && "block is not initialized!");
        assert(available_memory_ + (buffer_ - internal_buffer_) <= full_size_);

        std::unique_ptr<block_t> splited_block = create_initialize(resource_, full_size_);

        // calculate mid point (size wise)
        size_t accumulated_size = 0;
        item_metadata* split_item = last_item_metadata_;
        for(; split_item < end_ && accumulated_size < full_size_ / 2; split_item++) {
            accumulated_size += split_item->size + item_metadata_size;
        }
        if (split_item != end_) {
            accumulated_size += split_item->size + item_metadata_size;
        }

        bool append_possible;
        bool this_block;
        if (split_item == end_ && (first_id() < id && last_id() > id || *count_ < 2)) {
            append_possible = false;
        } else if (split_item->id < id) {
            // will go into second block
            append_possible = full_size_ - item_metadata_size > buffer_size + accumulated_size;
            this_block = false;
        } else {
            // will go into first block
            append_possible = available_memory_ + accumulated_size > buffer_size + item_metadata_size;
            this_block = true;
        }

        if (append_possible) {
            if (split_item == end_) {
                std::memcpy(splited_block->internal_buffer_, internal_buffer_, full_size_);
                splited_block->restore_block();
                reset();
            } else {
                for (; split_item >= last_item_metadata_; ) {
                    splited_block->append(split_item->id, internal_buffer_ + split_item->offset, split_item->size);
                    // nesessary to avoid buffer fragmentation 
                    // TODO: ??? move intems in chunks (actual data is consecutive inside each chunk)
                    remove(split_item->id);
                }
            }

            if (this_block) {
                append(id, append_buffer, buffer_size);
            } else {
                splited_block->append(id, append_buffer, buffer_size);
            }

            return {std::move(splited_block), nullptr};
        }

        split_item = find_item_(id);
        if (split_item == end_ || (split_item != end_ && split_item->id < id)) {
            split_item--;
        }
        for (; split_item >= last_item_metadata_; ) {
            splited_block->append(split_item->id, internal_buffer_ + split_item->offset, split_item->size);
            // nesessary to avoid buffer fragmentation 
            // TODO: ??? move intems in chunks (actual data is consecutive inside each chunk)
            remove(split_item->id);
        }
        if (append(id, append_buffer, buffer_size)) {
            return {std::move(splited_block), nullptr};
        }
        if (splited_block->append(id, append_buffer, buffer_size)) {
            return {std::move(splited_block), nullptr};
        }

        std::unique_ptr<block_t> middle_block = create_initialize(resource_, align_to_block_size(buffer_size + header_size + item_metadata_size));
        middle_block->append(id, append_buffer, buffer_size); // always true
        return {std::move(middle_block), std::move(splited_block)};
    }

    std::unique_ptr<block_t> block_t::split(size_t count) {
        assert(is_valid_ && "block is not initialized!");
        assert(count <= *count_);

        std::unique_ptr<block_t> splited_block = create_initialize(resource_, full_size_);
        
        if (count != 0) {
            for (auto split_item = last_item_metadata_ + count - 1; split_item >= last_item_metadata_; ) {
                splited_block->append(split_item->id, internal_buffer_ + split_item->offset, split_item->size);
                // nesessary to avoid buffer fragmentation 
                // TODO: ??? move intems in chunks (actual data is consecutive inside each chunk)
                remove(split_item->id);
            }
        }
        return splited_block;
    }

    void block_t::merge(std::unique_ptr<block_t> block) {
        assert(is_valid_ && "block is not initialized!");
        assert(block->is_valid_ && "block is not initialized!");
        assert(available_memory_ >= block->occupied_memory());
        assert(available_memory_ + (buffer_ - internal_buffer_) <= full_size_);
        if (block->count() == 0) {
            return;
        }

        if (block->first_id() > last_id()) {
            size_t delta_offset = (buffer_ - internal_buffer_) - header_size;
            size_t additional_offset = (block->buffer_ - block->internal_buffer_) - header_size;
            std::memcpy(buffer_, block->internal_buffer_ + header_size, (block->buffer_ - block->internal_buffer_) - header_size);
            buffer_ += additional_offset;
            std::memcpy(last_item_metadata_ - block->count(), block->last_item_metadata_, item_metadata_size * block->count());
            for(size_t i = 0; i < block->count(); i++) {
                (--last_item_metadata_)->offset += delta_offset;
            }
            available_memory_ -= additional_offset + item_metadata_size * block->count();
            *count_ += block->count();
        } else if (block->last_id() < first_id()) {
            size_t delta_offset = (buffer_ - internal_buffer_) - header_size;
            size_t additional_offset = (block->buffer_ - block->internal_buffer_) - header_size;
            std::memcpy(buffer_, block->internal_buffer_ + header_size, additional_offset);
            buffer_ += additional_offset;
            std::memcpy(last_item_metadata_ - block->count(), last_item_metadata_, item_metadata_size * block->count());
            std::memcpy(last_item_metadata_, block->last_item_metadata_, item_metadata_size * block->count());
            for(item_metadata* it = last_item_metadata_; it < end_; it++) {
                it->offset += delta_offset;
            }
            last_item_metadata_ -= block->count();
            available_memory_ -= additional_offset + item_metadata_size * block->count();
            *count_ += block->count();
        } else {
            // there is range overlaping and cannot be copied trivially
            for(item_metadata* it = block->end_ - 1; it >= block->last_item_metadata_; it--) {
                append(it->id, block->internal_buffer_ + it->offset, it->size);
            }
        }
    }

    void block_t::recalculate_checksum() {
        *checksum_ = calculate_checksum_();
    }

    bool block_t::varify_checksum() const {
        return *checksum_ == calculate_checksum_();
    }

    block_t::item_metadata* block_t::find_item_(uint64_t id) const {
        if (!is_valid_) {
            return end_;
        }
        return std::lower_bound(last_item_metadata_, end_, id, [](item_metadata& item, uint64_t id) { return item.id > id; });
    }

    uint64_t block_t::calculate_checksum_() const {
        assert(is_valid_ && "block is not initialized!");
        uint64_t size = full_size_ - (reinterpret_cast<data_ptr_t>(count_) - internal_buffer_);
        uint64_t result = 5953;
        // removing item can only change pointers location and item count, so count_ is included in checksum
        uint64_t* window = count_;
        for (uint64_t i = 0; i < size / 8; i++) {
            result ^= window[i] * 13315146811210211749;
        }
        return result;
    }

} // namespace core::b_plus_tree