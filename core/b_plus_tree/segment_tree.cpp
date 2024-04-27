#include "segment_tree.hpp"
#include <algorithm>
#include <cstring>

namespace core::b_plus_tree {
    segment_tree_t::iterator::iterator(segment_tree_t* seg_tree, segment_tree_t::block_metadata* metadata)
        : seg_tree_(seg_tree)
        , metadata_(metadata) {
        get_block();
    }

    void segment_tree_t::iterator::get_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            block_ = seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].block.get();
            seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].last_used = std::chrono::system_clock::now();
        } else {
            block_ = nullptr;
        }
    }
    void segment_tree_t::iterator::load_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            if (!seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].block.get()) {
                seg_tree_->load_segment_(metadata_);
                block_ = seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].block.get();
            }
        } else {
            assert(false && "segment_tree::iterator: out of range");
        }
    }

    segment_tree_t::r_iterator::r_iterator(segment_tree_t* seg_tree, segment_tree_t::block_metadata* metadata)
        : seg_tree_(seg_tree)
        , metadata_(metadata) {
        get_block();
    }

    void segment_tree_t::r_iterator::get_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            block_ = seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].block.get();
            seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].last_used = std::chrono::system_clock::now();
        } else {
            block_ = nullptr;
        }
    }
    void segment_tree_t::r_iterator::load_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            if (!seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].block.get()) {
                seg_tree_->load_segment_(metadata_);
                block_ = seg_tree_->segments_[metadata_ - seg_tree_->metadata_begin_].block.get();
            }
        } else {
            assert(false && "segment_tree::r_iterator: out of range");
        }
    }

    segment_tree_t::segment_tree_t(std::pmr::memory_resource* resource,
                                   std::unique_ptr<core::filesystem::file_handle_t> file)
        : resource_(resource)
        , file_(std::move(file)) {
        header_ = static_cast<size_t*>(resource_->allocate(header_size));
        item_count_ = header_ + 1;
        *header_ = 0;
        *item_count_ = 0;
        metadata_begin_ = reinterpret_cast<block_metadata*>(item_count_ + 1);
        metadata_end_ = metadata_begin_;
    }

    segment_tree_t::~segment_tree_t() {
        file_.reset();
        resource_->deallocate(static_cast<void*>(header_), header_size);
    }

    bool segment_tree_t::append(uint64_t id, const_data_ptr_t append_buffer, size_t buffer_size) {
        if (segments_.size() == 0) {
            insert_segment_(segments_.end(), construct_new_node_(id, append_buffer, buffer_size));
        } else {
            // reserve +2 items to be sure that iterators won't be invalidated
            if (segments_.size() + 2 < segments_.capacity()) {
                segments_.reserve(segments_.size() * 2 + 1);
            }

            block_metadata* metadata =
                std::upper_bound(metadata_begin_, metadata_end_, id, [](uint64_t id, block_metadata& m) {
                    return id < m.max_id;
                });
            metadata = (metadata == metadata_begin_ || (metadata != metadata_end_ && metadata->min_id < id))
                           ? metadata
                           : --metadata;
            segment_tree_t::it append_node = segments_.begin() + (metadata - metadata_begin_);
            if (!(*append_node).block) {
                load_segment_(metadata);
            }

            if ((*append_node).block->contains(id)) {
                return false;
            }
            if ((*append_node).block->is_memory_available(buffer_size)) {
                (*append_node).block->append(id, append_buffer, buffer_size);
                (*append_node).last_used = std::chrono::system_clock::now();
                (*append_node).modified = true;
                metadata->min_id = (*append_node).block->first_id();
                metadata->max_id = (*append_node).block->last_id();
            } else {
                // check if doc can go to the next block
                if ((*append_node).block->last_id() < id) {
                    // try block after it
                    append_node++;
                    metadata++;

                    if (append_node == segments_.end()) {
                        insert_segment_(append_node, construct_new_node_(id, append_buffer, buffer_size));
                    } else {
                        if (!(*append_node).block) {
                            load_segment_(metadata);
                        }
                        if ((*append_node).block->is_memory_available(buffer_size)) {
                            (*append_node).block->append(id, append_buffer, buffer_size);
                            (*append_node).last_used = std::chrono::system_clock::now();
                            (*append_node).modified = true;
                            metadata->min_id = (*append_node).block->first_id();
                            metadata->max_id = (*append_node).block->last_id();
                        } else {
                            insert_segment_(append_node, construct_new_node_(id, append_buffer, buffer_size));
                        }
                    }
                } else {
                    // nothing left but to split this block
                    auto split_result = (*append_node).block->split_append(id, append_buffer, buffer_size);
                    (*append_node).last_used = std::chrono::system_clock::now();
                    (*append_node).modified = true;
                    metadata->min_id = (*append_node).block->first_id();
                    metadata->max_id = (*append_node).block->last_id();
                    if (split_result.second) {
                        insert_segment_(append_node + 1,
                                        node_t{std::move(split_result.second), std::chrono::system_clock::now(), true});
                    }
                    insert_segment_(append_node + 1,
                                    node_t{std::move(split_result.first), std::chrono::system_clock::now(), true});
                }
            }
        }

        (*item_count_)++;
        return true;
    }

    bool segment_tree_t::remove(uint64_t id) {
        if (segments_.size() == 0) {
            return false;
        }

        block_metadata* metadata =
            std::lower_bound(metadata_begin_, metadata_end_, id, [](block_metadata& m, uint64_t id) {
                return m.max_id < id;
            });
        segment_tree_t::it remove_node = segments_.begin() + (metadata - metadata_begin_);

        if (!(*remove_node).block) {
            load_segment_(metadata);
        }

        if (remove_node == segments_.end() || !(*remove_node).block->contains(id)) {
            return false;
        }

        (*remove_node).block->remove(id);

        if ((*remove_node).block->count() == 0) {
            remove_segment_(remove_node);
            (*item_count_)--;
            return true;
        }

        (*remove_node).modified = true;
        metadata->min_id = (*remove_node).block->first_id();
        metadata->max_id = (*remove_node).block->last_id();
        if ((static_cast<double>((*remove_node).block->available_memory()) /
             static_cast<double>((*remove_node).block->block_size())) > merge_check) {
            it left = remove_node == segments_.begin() ? segments_.end() : remove_node - 1;
            it right = remove_node + 1;

            if (left != segments_.end() && !(*left).block) {
                load_segment_(metadata - 1);
            }
            if (right != segments_.end() && !(*right).block) {
                load_segment_(metadata + 1);
            }

            if (right != segments_.end() &&
                (*remove_node).block->available_memory() >= (*right).block->occupied_memory()) {
                (*remove_node).modified = true;
                (*remove_node).block->merge(std::move((*right).block));
                metadata->min_id = (*remove_node).block->first_id();
                metadata->max_id = (*remove_node).block->last_id();
                remove_segment_(right);
            } else if (left != segments_.end() &&
                       (*left).block->available_memory() >= (*remove_node).block->occupied_memory()) {
                (*left).block->merge(std::move((*remove_node).block));
                (*left).modified = true;
                (metadata - 1)->min_id = (*left).block->first_id();
                (metadata - 1)->max_id = (*left).block->last_id();
                remove_segment_(remove_node);
            }
        }
        (*item_count_)--;
        return true;
    }

    [[nodiscard]] std::unique_ptr<segment_tree_t>
    segment_tree_t::split(std::unique_ptr<core::filesystem::file_handle_t> file) {
        // make no sense to split tree with 0 blocks
        assert(metadata_begin_ != metadata_end_);
        assert(*item_count_ > 1);

        std::unique_ptr<segment_tree_t> splited_tree = std::make_unique<segment_tree_t>(resource_, std::move(file));

        size_t split_size = *item_count_ / 2;
        for (auto metadata = metadata_end_ - 1; metadata >= metadata_begin_ && split_size > 0; metadata--) {
            segment_tree_t::it node = segments_.begin() + (metadata - metadata_begin_);
            if (!(*node).block) {
                load_segment_(metadata);
            }

            size_t count = (*node).block->count();
            if (count <= split_size) {
                // move this block to splited_tree
                (*node).modified = true;
                splited_tree->insert_segment_(splited_tree->segments_.begin(), std::move(*node));
                remove_segment_(node);
                split_size -= count;
                *item_count_ -= count;
                *(splited_tree->item_count_) += count;
            } else {
                // split required amount from that block and break the loop
                splited_tree->insert_segment_(
                    splited_tree->segments_.begin(),
                    segment_tree_t::node_t{(*node).block->split(split_size), std::chrono::system_clock::now(), true});
                metadata->max_id = (*node).block->last_id();
                *item_count_ -= split_size;
                *(splited_tree->item_count_) += split_size;
                break;
            }
        }

        return splited_tree;
    }

    void segment_tree_t::balance_with(std::unique_ptr<segment_tree_t>& other) {
        assert(min_id() > other->max_id() || max_id() < other->min_id());
        assert(*item_count_ != *(other->item_count_) && *item_count_ != 0 && *(other->item_count_) != 0);
        // easier to check it where it is needed, then to add 2 new cases for it
        assert(*item_count_ < *(other->item_count_));

        size_t rebalance_size = (*item_count_ + *(other->item_count_)) / 2 - *item_count_;
        if (min_id() > other->max_id()) {
            for (auto metadata = other->metadata_end_ - 1; metadata >= other->metadata_begin_ && rebalance_size > 0;
                 metadata--) {
                segment_tree_t::it node = other->segments_.begin() + (metadata - other->metadata_begin_);
                if (!(*node).block) {
                    other->load_segment_(metadata);
                }

                size_t count = (*node).block->count();
                if (count <= rebalance_size) {
                    // move this block
                    (*node).modified = true;
                    insert_segment_(segments_.begin(), std::move(*node));
                    other->remove_segment_(node);
                    rebalance_size -= count;
                    *item_count_ += count;
                    *(other->item_count_) -= count;
                } else {
                    // split required amount from that block and break the loop
                    insert_segment_(segments_.begin(),
                                    segment_tree_t::node_t{(*node).block->split(rebalance_size),
                                                           std::chrono::system_clock::now(),
                                                           true});
                    assert(segments_.begin()->block->count() != 0 && "incorrect node split");
                    assert((*node).block->count() != 0 && "incorrect node split");
                    metadata->max_id = (*node).block->last_id();
                    *item_count_ += rebalance_size;
                    *(other->item_count_) -= rebalance_size;
                    break;
                }
            }
        } else {
            for (auto metadata = other->metadata_begin_; metadata < other->metadata_end_ && rebalance_size > 0;) {
                segment_tree_t::it node = other->segments_.begin() + (metadata - other->metadata_begin_);
                if (!(*node).block) {
                    other->load_segment_(metadata);
                }

                size_t count = (*node).block->count();
                if (count <= rebalance_size) {
                    // move this block
                    (*node).modified = true;
                    insert_segment_(segments_.end(), std::move(*node));
                    other->remove_segment_(node);
                    rebalance_size -= count;
                    *item_count_ += count;
                    *(other->item_count_) -= count;
                } else {
                    // split required amount from that block and break the loop
                    std::unique_ptr<block_t> temp_block_ptr = (*node).block->split(count - rebalance_size);
                    assert(temp_block_ptr->count() != 0 && "incorrect node split");
                    assert((*node).block->count() != 0 && "incorrect node split");
                    insert_segment_(
                        segments_.end(),
                        segment_tree_t::node_t{std::move((*node).block), std::chrono::system_clock::now(), true});
                    (*node).block = std::move(temp_block_ptr);
                    (*node).modified = true;
                    metadata->min_id = (*node).block->first_id();
                    *item_count_ += rebalance_size;
                    *(other->item_count_) -= rebalance_size;
                    break;
                }
            }
        }
    }

    void segment_tree_t::merge(std::unique_ptr<segment_tree_t>& other) {
        assert(min_id() > other->max_id() || max_id() < other->min_id());
        assert(*item_count_ != 0 && *(other->item_count_) != 0);

        if (min_id() > other->max_id()) {
            // insert all at begin pos
            while (other->segments_.size() != 0) {
                if (!(other->segments_.end() - 1)->block) {
                    other->load_segment_(other->metadata_end_ - 1);
                }
                insert_segment_(segments_.begin(), std::move(*(other->segments_.end() - 1)));
                segments_.begin()->modified = true;
                *item_count_ += segments_.begin()->block->count();
                other->remove_segment_(other->segments_.end() - 1);
            }
        } else {
            // insert all at end pos
            while (other->segments_.size() != 0) {
                if (!other->segments_.begin()->block) {
                    other->load_segment_(other->metadata_begin_);
                }
                other->segments_.begin()->modified = true;
                *item_count_ += other->segments_.begin()->block->count();
                insert_segment_(segments_.end(), std::move(*(other->segments_.begin())));
                other->remove_segment_(other->segments_.begin());
            }
        }
        *(other->item_count_) = 0;
    }

    bool segment_tree_t::contains(uint64_t id) {
        block_metadata* metadata =
            std::lower_bound(metadata_begin_, metadata_end_, id, [](block_metadata& m, uint64_t id) {
                return m.max_id < id;
            });
        auto search_node = segments_.begin() + (metadata - metadata_begin_);
        if (search_node != segments_.end() && !(*search_node).block) {
            load_segment_(metadata);
        }
        return search_node == segments_.cend() ? false : (*search_node).block->contains(id);
    }

    size_t segment_tree_t::size_of(uint64_t id) {
        block_metadata* metadata =
            std::lower_bound(metadata_begin_, metadata_end_, id, [](block_metadata& m, uint64_t id) {
                return m.max_id < id;
            });
        segment_tree_t::it search_node = segments_.begin() + (metadata - metadata_begin_);
        if (search_node != segments_.end() && !(*search_node).block) {
            load_segment_(metadata);
        }
        if (search_node == segments_.cend()) {
            return 0;
        } else {
            return (*search_node).block->size_of(id);
        }
    }

    data_ptr_t segment_tree_t::data_of(uint64_t id) {
        block_metadata* metadata =
            std::lower_bound(metadata_begin_, metadata_end_, id, [](block_metadata& m, uint64_t id) {
                return m.max_id < id;
            });
        segment_tree_t::it search_node = segments_.begin() + (metadata - metadata_begin_);
        if (search_node != segments_.end() && !(*search_node).block) {
            load_segment_(metadata);
        }
        if (search_node == segments_.cend()) {
            return nullptr;
        } else {
            return (*search_node).block->data_of(id);
        }
    }

    std::pair<data_ptr_t, size_t> segment_tree_t::get_item(uint64_t id) {
        block_metadata* metadata =
            std::lower_bound(metadata_begin_, metadata_end_, id, [](block_metadata& m, uint64_t id) {
                return m.max_id < id;
            });
        segment_tree_t::it search_node = segments_.begin() + (metadata - metadata_begin_);
        if (search_node != segments_.end() && !(*search_node).block) {
            load_segment_(metadata);
        }
        if (search_node == segments_.cend()) {
            return {nullptr, 0};
        } else {
            return (*search_node).block->get_item(id);
        }
    }

    uint64_t segment_tree_t::min_id() const {
        if (metadata_begin_ == metadata_end_) {
            return 0;
        } else {
            return metadata_begin_->min_id;
        }
    }

    uint64_t segment_tree_t::max_id() const {
        if (metadata_begin_ == metadata_end_) {
            return INVALID_ID;
        } else {
            return (metadata_end_ - 1)->max_id;
        }
    }

    size_t segment_tree_t::blocks_count() const { return segments_.size(); }

    size_t segment_tree_t::item_count() const { return *item_count_; }

    void segment_tree_t::flush() {
        close_gaps_();

        /*  header_  */
        file_->write(static_cast<void*>(header_), header_size, 0);

        /*  BLOCKS  */
        // TODO: it would be faster to flush blocks in offset order, instead of their id
        block_metadata* metadata = metadata_begin_;
        size_t file_size = header_size;
        for (auto segment = segments_.begin(); segment != segments_.end(); segment++, metadata++) {
            file_size += metadata->size;
            // if segment is not loaded, it does not have to be flushed
            if ((*segment).block.get()) {
                assert((*segment).block->count() != 0 && "block is empty");
                if ((*segment).modified) {
                    (*segment).block->recalculate_checksum();
                    file_->write((*segment).block->internal_buffer(), metadata->size, metadata->file_offset);
                    (*segment).modified = false;
                }
            }
        }
        file_->truncate(static_cast<int64_t>(file_size));
        file_->sync();
    }

    void segment_tree_t::clean_load() {
        segments_.clear();
        file_->seek(0);
        file_->read(static_cast<void*>(header_), header_size);
        metadata_end_ = metadata_begin_ + *header_;
        gap_tracker_.init(file_->file_size(), INVALID_SIZE);

        segments_.reserve(*header_);
        // TODO: it would be faster to load blocks in offset order, instead of their id
        for (block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            // call directly because there is no need to modify header
            segments_.emplace_back(
                node_t{create_initialize(resource_, metadata->size), std::chrono::system_clock::now(), false});
            file_->read(segments_.back().block->internal_buffer(), metadata->size, metadata->file_offset);
            segments_.back().block->restore_block();
            assert(segments_.back().block->varify_checksum() && "block was modified outside of segment tree");
            assert(segments_.back().block->count() != 0 && "block is empty");
        }
    }

    void segment_tree_t::lazy_load() {
        segments_.clear();
        file_->seek(0);
        file_->read(static_cast<void*>(header_), header_size);
        metadata_end_ = metadata_begin_ + *header_;
        gap_tracker_.init(file_->file_size(), INVALID_SIZE);

        segments_.reserve(*header_);
        for (size_t i = 0; i < *header_; i++) {
            segments_.emplace_back(node_t{nullptr, std::chrono::system_clock::now(), false});
        }
    }

    segment_tree_t::node_t
    segment_tree_t::construct_new_node_(uint64_t id, const_data_ptr_t append_buffer, size_t buffer_size) {
        std::unique_ptr<block_t> new_block_ptr;
        try {
            new_block_ptr = create_initialize(
                resource_,
                align_to_block_size(buffer_size + block_t::header_size + block_t::item_metadata_size));
        } catch (...) {
            unload_old_segments_();
            new_block_ptr = create_initialize(
                resource_,
                align_to_block_size(buffer_size + block_t::header_size + block_t::item_metadata_size));
        }
        new_block_ptr->append(id, append_buffer, buffer_size); // always true
        return {std::move(new_block_ptr), std::chrono::system_clock::now(), true};
    }

    void segment_tree_t::load_segment_(block_metadata* metadata) {
        segment_tree_t::it node = segments_.begin() + (metadata - metadata_begin_);

        // if there is not enough memory, flush old blocks
        try {
            (*node).block = create_initialize(resource_, metadata->size);
        } catch (...) {
            unload_old_segments_();
            (*node).block = create_initialize(resource_, metadata->size);
        }

        file_->read((*node).block->internal_buffer(), metadata->size, metadata->file_offset);
        (*node).block->restore_block();
        assert((*node).block->varify_checksum() && "block was modified outside of segment tree");
        (*node).last_used = std::chrono::system_clock::now();
        (*node).modified = false;
    }

    void segment_tree_t::unload_old_segments_() {
        close_gaps_();
        std::vector<std::pair<std::chrono::time_point<std::chrono::system_clock>, size_t>> blocks_to_unload;
        blocks_to_unload.reserve(segments_.size());
        for (size_t i = 0; i < segments_.size(); i++) {
            if (segments_[i].block.get() != nullptr) {
                blocks_to_unload.emplace_back(segments_[i].last_used, i);
            }
        }
        std::sort(blocks_to_unload.begin(), blocks_to_unload.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
        size_t half_size = blocks_to_unload.size() / 2;
        for (size_t i = 0; i < half_size; i++) {
            size_t num = blocks_to_unload[i].second;
            segments_[num].block->recalculate_checksum();
            file_->write(segments_[num].block->internal_buffer(),
                         (metadata_begin_ + num)->size,
                         (metadata_begin_ + num)->file_offset);
            segments_[num].block = nullptr;
            segments_[num].modified = false;
        }
    }

    void segment_tree_t::insert_segment_(it pos, node_t&& node) {
        node.last_used = std::chrono::system_clock::now();
        auto id = pos - segments_.begin();
        block_metadata* metadata = metadata_begin_ + id;
        std::memmove(metadata + 1, metadata, (segments_.size() - static_cast<uint32_t>(id)) * block_metadata_size);
        metadata->file_offset = gap_tracker_.find_gap(node.block->block_size());
        metadata->size = node.block->block_size();
        metadata->min_id = node.block->first_id();
        metadata->max_id = node.block->last_id();
        metadata_end_++;

        segments_.insert(pos, std::move(node));
        *header_ = segments_.size();
    }

    void segment_tree_t::remove_segment_(it pos) {
        auto id = pos - segments_.begin();
        block_metadata* metadata = metadata_begin_ + id;
        gap_tracker_.remove_gap({metadata->file_offset, metadata->size});
        std::memmove(metadata, metadata + 1, (segments_.size() - static_cast<uint32_t>(id)) * block_metadata_size);
        metadata_end_--;

        segments_.erase(pos);
        *header_ = segments_.size();
    }

    void segment_tree_t::close_gaps_() {
        auto& gaps = gap_tracker_.empty_spaces();
        while (gaps.size() > 1) {
            // TODO: try to close gaps with existing blocks
            size_t i = 0;
            for (block_metadata* it = metadata_begin_; it < metadata_end_; it++, i++) {
                if (it->file_offset > gaps.front().offset) {
                    it->file_offset -= gaps.front().size;
                    segments_[i].modified = true;
                }
            }
            gaps.erase(gaps.begin());
        }
    }

} // namespace core::b_plus_tree