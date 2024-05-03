#include "block.hpp"
#include <algorithm>
#include <cassert>
#include <crc32c/crc32c.h>
#include <cstring>

namespace core::b_plus_tree {
    size_t SECTOR_SIZE = 4096;

    block_t::iterator::iterator(const block_t* block, block_t::list_metadata* metadata)
        : block_(block)
        , metadata_(metadata) {
        rebuild_data();
    }
    void block_t::iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_list_metadata_) {
            data_.id = metadata_->id;
            data_.items.clear();
            data_ptr_t list = block_->internal_buffer_ + metadata_->offset;
            size_t* list_count = reinterpret_cast<size_t*>(list);
            data_.items.reserve(*list);
            for (size_t i = 1; i < *list_count; i++) {
                data_.items.emplace_back(list + *(list_count + i), *(list_count + i + 1) - *(list_count + i));
            }
            data_.items.emplace_back(list + *(list_count + *list_count), metadata_->size - *(list_count + *list_count));
        } else {
            data_.id = INVALID_ID;
            data_.items.clear();
        }
    }

    block_t::r_iterator::r_iterator(const block_t* block, block_t::list_metadata* metadata)
        : block_(block)
        , metadata_(metadata) {
        rebuild_data();
    }
    void block_t::r_iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_list_metadata_) {
            data_.id = metadata_->id;
            data_.items.clear();
            data_ptr_t list = block_->internal_buffer_ + metadata_->offset;
            size_t* list_count = reinterpret_cast<size_t*>(list);
            data_.items.reserve(*list);
            for (size_t i = 1; i < *list_count; i++) {
                data_.items.emplace_back(list + *(list_count + i), *(list_count + i + 1) - *(list_count + i));
            }
            data_.items.emplace_back(list + *(list_count + *list_count), metadata_->size - *(list_count + *list_count));
        } else {
            data_.id = INVALID_ID;
            data_.items.clear();
        }
    }

    block_t::block_t(std::pmr::memory_resource* resource)
        : resource_(resource) {}

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
        checksum_ = reinterpret_cast<size_t*>(internal_buffer_);
        count_ = checksum_ + 1;
        *count_ = 0;
        unique_id_count_ = checksum_ + 2;
        *unique_id_count_ = 0;
        end_ = reinterpret_cast<list_metadata*>(internal_buffer_ + size);

        restore_block();
    }

    size_t block_t::available_memory() const { return available_memory_; }

    size_t block_t::occupied_memory() const { return full_size_ - available_memory_ - header_size; }

    bool block_t::is_memory_available(size_t request_size) const {
        return request_size + list_metadata_size + item_metadata_size <= available_memory_;
    }

    bool block_t::is_empty() const { return last_list_metadata_ == end_; }

    bool block_t::is_valid() const { return is_valid_; }

    bool block_t::append(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        assert(is_valid_ && "block is not initialized!");
        if (!is_memory_available(buffer_size)) {
            return false;
        }

        auto list_metadata = find_item_metadata_(id);
        if (list_metadata != end_ && list_metadata->id == id) {
            // id is present, but item may be not
            data_ptr_t list = internal_buffer_ + list_metadata->offset;
            size_t* list_count = reinterpret_cast<size_t*>(list);
            size_t item_size;
            for (size_t i = 1; i < *list_count; i++) {
                item_size = *(list_count + i + 1) - *(list_count + i);
                if (item_size != buffer_size) {
                    continue;
                } else {
                    if (std::memcmp(list + *(list_count + i), buffer, item_size) == 0) {
                        return false;
                    }
                }
            }
            // check last item in list_count
            size_t offset = *(list_count + *list_count);
            item_size = list_metadata->size - offset;
            if (item_size == buffer_size && std::memcmp(list + offset, buffer, item_size) == 0) {
                return false;
            }
            // since items inside list_count are not sorted, it is easier to add to start
            // clear space for item
            data_ptr_t pos = internal_buffer_ + list_metadata->offset + (*list_count + 1) * sizeof(size_t);
            item_size = buffer_size + sizeof(size_t);
            std::memmove(pos + item_size, pos, buffer_ - pos);
            // adjust metadata
            offset = pos - internal_buffer_;
            for (auto it = last_list_metadata_; it != end_; it++) {
                if (it->offset > offset) {
                    it->offset += item_size;
                }
            }
            // adjust local offsets
            std::memmove(list_count + 2, list_count + 1, *list_count * sizeof(size_t));
            for (size_t i = 2; i <= *list_count + 1; i++) {
                *(list_count + i) += item_size;
            }
            (*list_count)++;
            *(list_count + 1) = (*list_count + 1) * sizeof(size_t);
            // finally insert item
            std::memcpy(list + *(list_count + 1), buffer, buffer_size);
            buffer_ += item_size;
            available_memory_ -= item_size;
            list_metadata->size += item_size;

        } else {
            std::memmove(last_list_metadata_ - 1,
                         last_list_metadata_,
                         (list_metadata - last_list_metadata_) * list_metadata_size);
            list_metadata--;
            last_list_metadata_--;
            list_metadata->id = id;
            list_metadata->offset = buffer_ - internal_buffer_;
            list_metadata->size = buffer_size + item_metadata_size;

            size_t* list_data = reinterpret_cast<size_t*>(buffer_);
            // new id, list_count will be 1
            *list_data = 1;
            // to get start of actual data add that to offset from metadata
            *(list_data + 1) = sizeof(size_t) * 2;
            std::memcpy(list_data + 2, buffer, buffer_size);
            buffer_ += list_metadata->size;
            available_memory_ -= list_metadata->size + list_metadata_size;
            (*unique_id_count_)++;
        }

        (*count_)++;
        return true;
    }

    bool block_t::remove(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);

        if (list_metadata == end_ || list_metadata->id != id) {
            return false;
        }

        if (list_metadata != end_ && list_metadata->id == id) {
            // id is present, but item may be not
            data_ptr_t list = internal_buffer_ + list_metadata->offset;
            size_t* list_count = reinterpret_cast<size_t*>(list);

            if (*list_count == 1) {
                size_t offset = *(list_count + *list_count);
                size_t item_size = list_metadata->size - offset;
                if (item_size != buffer_size || std::memcmp(list + offset, buffer, item_size) != 0) {
                    return false;
                }
                size_t chunk_size = list_metadata->size;
                data_ptr_t chunk_start = internal_buffer_ + list_metadata->offset;
                data_ptr_t next_chunk_start = chunk_start + list_metadata->size;
                std::memmove(chunk_start, next_chunk_start, buffer_ - next_chunk_start);
                buffer_ -= list_metadata->size;
                available_memory_ += list_metadata->size + list_metadata_size;
                std::memmove(last_list_metadata_ + 1,
                             last_list_metadata_,
                             (list_metadata - last_list_metadata_) * list_metadata_size);
                last_list_metadata_++;

                offset = chunk_start - internal_buffer_;
                for (auto it = last_list_metadata_; it != end_; it++) {
                    if (it->offset > offset) {
                        it->offset -= chunk_size;
                    }
                }
                (*unique_id_count_)--;
            } else {
                size_t offset;
                size_t item_size;
                size_t i = 1;
                bool is_present = false;
                for (; i < *list_count; i++) {
                    offset = *(list_count + i);
                    item_size = *(list_count + i + 1) - offset;
                    if (item_size != buffer_size) {
                        continue;
                    } else {
                        if (std::memcmp(list + offset, buffer, item_size) == 0) {
                            is_present = true;
                            break;
                        }
                    }
                }
                // check last item in list_count
                if (!is_present) {
                    offset = *(list_count + *list_count);
                    item_size = list_metadata->size - offset;
                    if (item_size == buffer_size && std::memcmp(list + offset, buffer, item_size) == 0) {
                        is_present = true;
                    } else {
                        return false;
                    }
                }

                // item found, adjust local offsets after
                for (size_t j = 1; j < i; j++) {
                    *(list_count + j) -= sizeof(size_t);
                }
                size_t chunk_size = item_size + sizeof(size_t);
                for (size_t j = i + 1; j <= *list_count; j++) {
                    *(list_count + j) -= chunk_size;
                }
                // 2 gaps will appear. one in list offsets, second between items
                std::memmove(list_count + i, list_count + (i + 1), offset - sizeof(size_t) * (i + 1));
                offset -= sizeof(size_t);
                std::memmove(list + offset, list + offset + chunk_size, buffer_ - (list + offset + chunk_size));
                (*list_count)--;
                list_metadata->size -= chunk_size;
                buffer_ -= chunk_size;
                available_memory_ += chunk_size;

                for (auto it = last_list_metadata_; it != end_; it++) {
                    if (it->offset > list_metadata->offset + offset) {
                        it->offset -= chunk_size;
                    }
                }
            }
            (*count_)--;
            return true;
        } else {
            return false;
        }
    }

    bool block_t::remove_id(uint64_t id) {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);

        if (list_metadata == end_ || list_metadata->id != id) {
            return false;
        }

        size_t list_count = *reinterpret_cast<size_t*>(internal_buffer_ + list_metadata->offset);
        size_t chunk_size = list_metadata->size;
        data_ptr_t chunk_start = internal_buffer_ + list_metadata->offset;
        data_ptr_t next_chunk_start = chunk_start + list_metadata->size;
        std::memmove(chunk_start, next_chunk_start, buffer_ - next_chunk_start);
        buffer_ -= list_metadata->size;
        available_memory_ += list_metadata->size + list_metadata_size;
        std::memmove(last_list_metadata_ + 1,
                     last_list_metadata_,
                     (list_metadata - last_list_metadata_) * list_metadata_size);
        last_list_metadata_++;

        size_t offset = chunk_start - internal_buffer_;
        for (auto it = last_list_metadata_; it != end_; it++) {
            if (it->offset > offset) {
                it->offset -= chunk_size;
            }
        }

        *count_ -= list_count;
        (*unique_id_count_)--;
        return true;
    }

    bool block_t::contains_id(uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);
        return (list_metadata != end_ && list_metadata->id == id) ? true : false;
    }

    bool block_t::contains(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) const {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);

        if (list_metadata != end_ && list_metadata->id == id) {
            data_ptr_t list = internal_buffer_ + list_metadata->offset;
            size_t* list_count = reinterpret_cast<size_t*>(list);
            size_t offset;
            size_t item_size;
            for (size_t i = 1; i < *list_count; i++) {
                offset = *(list_count + i);
                item_size = *(list_count + i + 1) - offset;
                if (item_size != buffer_size) {
                    continue;
                } else {
                    if (std::memcmp(list + offset, buffer, item_size) == 0) {
                        return true;
                    }
                }
            }
            // check last item in list_count
            offset = *(list_count + *list_count);
            item_size = list_metadata->size - offset;
            if (item_size == buffer_size && std::memcmp(list + offset, buffer, item_size) == 0) {
                return true;
            }
        }
        return false;
    }

    size_t block_t::item_count(uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);

        if (list_metadata != end_ && list_metadata->id == id) {
            return *reinterpret_cast<size_t*>(internal_buffer_ + list_metadata->offset);
        } else {
            return 0;
        }
    }

    std::pair<data_ptr_t, size_t> block_t::get_item(uint64_t id, size_t index) const {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);

        if (list_metadata != end_ && list_metadata->id == id) {
            size_t* list = reinterpret_cast<size_t*>(internal_buffer_ + list_metadata->offset);
            assert(index < *list);
            if (index + 1 < *list) {
                return {internal_buffer_ + list_metadata->offset + *(list + index + 1),
                        *(list + index + 2) - *(list + index + 1)};
            } else {
                return {internal_buffer_ + list_metadata->offset + *(list + index + 1),
                        list_metadata->size - *(list + *list)};
            }
        }
        return {nullptr, 0};
    }

    void block_t::get_items(std::vector<std::pair<data_ptr_t, size_t>>& result, uint64_t id) const {
        assert(is_valid_ && "block is not initialized!");
        auto list_metadata = find_item_metadata_(id);

        if (list_metadata != end_ && list_metadata->id == id) {
            size_t* list = reinterpret_cast<size_t*>(internal_buffer_ + list_metadata->offset);
            for (size_t i = 1; i <= *list; i++) {
                if (i < *list) {
                    result.emplace_back(internal_buffer_ + *(list + i), *(list + i + 1) - *(list + i));
                } else {
                    result.emplace_back(internal_buffer_ + *(list + i), list_metadata->size - *(list + *list));
                }
            }
        }
    }

    size_t block_t::count() const {
        assert(is_valid_ && "block is not initialized!");
        return *count_;
    }

    size_t block_t::unique_id_count() const {
        assert(is_valid_ && "block is not initialized!");
        return *unique_id_count_;
    }

    uint64_t block_t::min_id() const {
        assert(is_valid_ && "block is not initialized!");
        return last_list_metadata_ == end_ ? INVALID_ID : (end_ - 1)->id;
    }
    uint64_t block_t::max_id() const {
        assert(is_valid_ && "block is not initialized!");
        return last_list_metadata_ == end_ ? INVALID_ID : last_list_metadata_->id;
    }

    data_ptr_t block_t::internal_buffer() {
        assert(is_valid_ && "block is not initialized!");
        return internal_buffer_;
    }

    size_t block_t::block_size() const { return full_size_; }

    void block_t::reset() {
        *count_ = 0;
        last_list_metadata_ = end_;
        restore_block();
    }

    void block_t::restore_block() {
        last_list_metadata_ = end_ - *unique_id_count_;
        buffer_ = internal_buffer_ + header_size;
        for (auto it = last_list_metadata_; it < end_; it++) {
            buffer_ += it->size;
        }
        available_memory_ = reinterpret_cast<data_ptr_t>(last_list_metadata_) - buffer_;
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
        std::memcpy(new_buffer + new_size - *unique_id_count_ * list_metadata_size,
                    last_list_metadata_,
                    *unique_id_count_ * list_metadata_size);

        size_t buffer_offset = buffer_ - internal_buffer_;
        resource_->deallocate(internal_buffer_, full_size_);
        internal_buffer_ = new_buffer;
        buffer_ = internal_buffer_ + buffer_offset;
        available_memory_ = new_size - full_size_;
        full_size_ = new_size;
        checksum_ = reinterpret_cast<size_t*>(internal_buffer_);
        count_ = checksum_ + 1;
        unique_id_count_ = checksum_ + 2;
        end_ = reinterpret_cast<list_metadata*>(internal_buffer_ + new_size);
        last_list_metadata_ = end_ - *unique_id_count_;
    }

    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>>
    block_t::split_append(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        // if append is possible, split in half, append to required part
        // if append is not possible, split by required id, try again
        // if it fails again, third block is required
        assert(is_valid_ && "block is not initialized!");

        std::unique_ptr<block_t> splited_block = create_initialize(resource_, full_size_);

        // calculate mid point (size wise)
        size_t accumulated_size = 0;
        list_metadata* split_item = last_list_metadata_;
        for (; split_item < end_ && accumulated_size < full_size_ / 2; split_item++) {
            accumulated_size += split_item->size + list_metadata_size;
        }
        if (split_item != end_) {
            accumulated_size += split_item->size + list_metadata_size;
        }

        bool append_possible;
        bool this_block;
        if (split_item == end_ && (min_id() <= id && max_id() >= id || *unique_id_count_ < 2)) {
            append_possible = false;
        } else if (split_item->id <= id) {
            // will go into second block
            append_possible = full_size_ - list_metadata_size > buffer_size + accumulated_size;
            this_block = false;
        } else {
            // will go into first block
            append_possible = available_memory_ + accumulated_size > buffer_size + list_metadata_size;
            this_block = true;
        }

        if (append_possible) {
            if (split_item == end_) {
                std::memcpy(splited_block->internal_buffer_, internal_buffer_, full_size_);
                splited_block->restore_block();
                reset();
            } else {
                for (; split_item >= last_list_metadata_;) {
                    // Append whole list
                    splited_block->append_list_(split_item->id,
                                                internal_buffer_ + split_item->offset,
                                                split_item->size);
                    // nesessary to avoid buffer fragmentation
                    // TODO: ??? move intems in chunks (actual data is consecutive inside each chunk)
                    remove_id(split_item->id);
                }
            }

            if (this_block) {
                append(id, buffer, buffer_size);
            } else {
                splited_block->append(id, buffer, buffer_size);
            }

            return {std::move(splited_block), nullptr};
        }

        split_item = find_item_metadata_(id);
        if (split_item == end_ || (split_item != last_list_metadata_ && split_item->id <= id)) {
            split_item--;
        }
        for (; split_item >= last_list_metadata_;) {
            // Append whole list
            splited_block->append_list_(split_item->id, internal_buffer_ + split_item->offset, split_item->size);
            // nesessary to avoid buffer fragmentation
            // TODO: ??? move intems in chunks (actual data is consecutive inside each chunk)
            remove_id(split_item->id);
        }
        if (max_id() >= id) {
            append(id, buffer, buffer_size);
            return {std::move(splited_block), nullptr};
        }
        if (splited_block->min_id() <= id) {
            splited_block->append(id, buffer, buffer_size);
            return {std::move(splited_block), nullptr};
        }

        std::unique_ptr<block_t> middle_block =
            create_initialize(resource_, align_to_block_size(buffer_size + header_size + list_metadata_size));
        middle_block->append(id, buffer, buffer_size);
        return {std::move(middle_block), std::move(splited_block)};
    }

    std::unique_ptr<block_t> block_t::split(size_t count) {
        assert(is_valid_ && "block is not initialized!");
        assert(count <= *count_);

        std::unique_ptr<block_t> splited_block = create_initialize(resource_, full_size_);

        if (count != 0) {
            for (auto split_item = last_list_metadata_ + count - 1; split_item >= last_list_metadata_;) {
                // Append whole list
                splited_block->append_list_(split_item->id, internal_buffer_ + split_item->offset, split_item->size);
                // nesessary to avoid buffer fragmentation
                // TODO: ??? move intems in chunks (actual data is consecutive inside each chunk)
                remove_id(split_item->id);
            }
        }
        return splited_block;
    }

    void block_t::merge(std::unique_ptr<block_t> other) {
        assert(is_valid_ && "other is not initialized!");
        assert(other->is_valid_ && "other block is not initialized!");
        assert(available_memory_ >= other->occupied_memory());
        assert(available_memory_ + (buffer_ - internal_buffer_) <= full_size_);
        if (other->count() == 0) {
            return;
        }

        if (other->min_id() > max_id()) {
            size_t delta_offset = (buffer_ - internal_buffer_) - header_size;
            size_t additional_offset = (other->buffer_ - other->internal_buffer_) - header_size;
            std::memcpy(buffer_, other->internal_buffer_ + header_size, additional_offset);
            buffer_ += additional_offset;
            std::memcpy(last_list_metadata_ - other->unique_id_count(),
                        other->last_list_metadata_,
                        list_metadata_size * other->unique_id_count());
            for (size_t i = 0; i < other->unique_id_count(); i++) {
                (--last_list_metadata_)->offset += delta_offset;
            }
            available_memory_ -= additional_offset + list_metadata_size * other->unique_id_count();
            *count_ += other->count();
            *unique_id_count_ += other->unique_id_count();
        } else if (other->max_id() < min_id()) {
            size_t delta_offset = (buffer_ - internal_buffer_) - header_size;
            size_t additional_offset = (other->buffer_ - other->internal_buffer_) - header_size;
            std::memcpy(buffer_, other->internal_buffer_ + header_size, additional_offset);
            buffer_ += additional_offset;
            std::memcpy(last_list_metadata_ - other->unique_id_count(),
                        last_list_metadata_,
                        list_metadata_size * other->unique_id_count());
            std::memcpy(last_list_metadata_, other->last_list_metadata_, list_metadata_size * other->unique_id_count());
            for (list_metadata* it = last_list_metadata_; it < end_; it++) {
                it->offset += delta_offset;
            }
            last_list_metadata_ -= other->unique_id_count();
            available_memory_ -= additional_offset + list_metadata_size * other->unique_id_count();
            *count_ += other->count();
            *unique_id_count_ += other->unique_id_count();
        } else {
            // there is range overlaping and cannot be copied trivially
            for (list_metadata* it = other->end_ - 1; it >= other->last_list_metadata_; it--) {
                append_list_(it->id, other->internal_buffer_ + it->offset, it->size);
            }
        }
    }

    void block_t::recalculate_checksum() { *checksum_ = calculate_checksum_(); }

    bool block_t::varify_checksum() const { return *checksum_ == calculate_checksum_(); }

    block_t::list_metadata* block_t::find_item_metadata_(uint64_t id) const {
        if (!is_valid_) {
            return end_;
        }
        return std::lower_bound(last_list_metadata_, end_, id, [](list_metadata& list_metadata, uint64_t id) {
            return list_metadata.id > id;
        });
    }

    bool block_t::append_list_(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        assert(is_valid_ && "block is not initialized!");
        if (!is_memory_available(buffer_size)) {
            return false;
        }

        auto list_metadata = find_item_metadata_(id);
        if (list_metadata != end_ && list_metadata->id == id) {
            return false;
        }

        std::memmove(last_list_metadata_ - 1,
                     last_list_metadata_,
                     (list_metadata - last_list_metadata_) * list_metadata_size);
        list_metadata--;
        last_list_metadata_--;
        list_metadata->id = id;
        list_metadata->offset = buffer_ - internal_buffer_;
        list_metadata->size = buffer_size;

        std::memcpy(buffer_, buffer, buffer_size);
        buffer_ += buffer_size;
        available_memory_ -= buffer_size + list_metadata_size;

        *count_ += *reinterpret_cast<const size_t*>(buffer);
        (*unique_id_count_)++;
        return true;
    }

    size_t block_t::calculate_checksum_() const {
        assert(is_valid_ && "block is not initialized!");
        data_ptr_t crc_buffer = reinterpret_cast<data_ptr_t>(count_);
        size_t size = full_size_ - (crc_buffer - internal_buffer_);
        return crc32c::Crc32c(crc_buffer, size);
    }

} // namespace core::b_plus_tree
