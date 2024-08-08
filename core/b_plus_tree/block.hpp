#pragma once

#include <cassert>
#include <cstdlib>
#include <memory>
#include <memory_resource>
#include <vector>

namespace core::b_plus_tree {

    typedef uint8_t data_t;
    typedef data_t* data_ptr_t;
    typedef const data_t* const_data_ptr_t;

    // The maximum block id is 2^62 so invalid id could be all ones
    constexpr uint64_t MAX_ID = 4611686018427387904;
    constexpr uint64_t INVALID_ID = uint64_t(-1);
    constexpr size_t INVALID_SIZE = size_t(-1);
    // The default block size
    constexpr size_t DEFAULT_BLOCK_SIZE = 262144; // 32 Kb

    // Align size (ceiling)
    static inline size_t align_to_block_size(size_t size) {
        return ((size + (DEFAULT_BLOCK_SIZE - 1)) / DEFAULT_BLOCK_SIZE) * DEFAULT_BLOCK_SIZE;
    }

    class block_t {
    private:
        struct item_metadata {
            uint64_t id;
            size_t offset;
            size_t size;
        };

    public:
        static constexpr size_t item_metadata_size = sizeof(item_metadata);
        static constexpr size_t header_size = sizeof(uint64_t) + sizeof(size_t);

        struct item_data {
            uint64_t id;
            data_ptr_t data;
            size_t size;
        };

        class iterator {
        public:
            iterator(const block_t* block, item_metadata* metadata);

            inline const item_data& operator*() const { return data_; }
            inline const item_data* operator->() const { return &data_; }

            // note: metadata is stored in reverse order, so iterator increment and decrement is reversed
            inline const iterator& operator++() {
                metadata_--;
                rebuild_data();
                return *this;
            }
            inline const iterator& operator--() {
                metadata_++;
                rebuild_data();
                return *this;
            }
            inline iterator operator++(int) {
                auto temp = iterator(block_, metadata_);
                metadata_--;
                rebuild_data();
                return temp;
            }
            inline iterator operator--(int) {
                auto temp = iterator(block_, metadata_);
                metadata_++;
                rebuild_data();
                return temp;
            }
            inline iterator operator+(int i) { return iterator(block_, metadata_ - i); }
            inline iterator operator-(int i) { return iterator(block_, metadata_ + i); }
            friend inline iterator operator+(int i, const iterator& rhs) {
                return iterator(rhs.block_, rhs.metadata_ - i);
            }
            friend inline iterator operator-(int i, const iterator& rhs) {
                return iterator(rhs.block_, rhs.metadata_ + i);
            }
            friend int operator-(const iterator& lhs, const iterator& rhs) {
                assert(lhs.block_ == rhs.block_);
                return rhs.metadata_ - lhs.metadata_;
            }

            inline iterator& operator=(const iterator& rhs) {
                metadata_ = rhs.metadata_;
                rebuild_data();
                return *this;
            }
            inline iterator& operator+=(int rhs) {
                metadata_ -= rhs;
                rebuild_data();
                return *this;
            }
            inline iterator& operator-=(int rhs) {
                metadata_ += rhs;
                rebuild_data();
                return *this;
            }

            inline bool operator==(const iterator& rhs) { return metadata_ == rhs.metadata_; }
            inline bool operator<(const iterator& rhs) { return metadata_ > rhs.metadata_; }
            inline bool operator>(const iterator& rhs) { return metadata_ < rhs.metadata_; }
            inline bool operator!=(const iterator& rhs) { return !(*this == rhs); }
            inline bool operator<=(const iterator& rhs) { return !(*this > rhs); }
            inline bool operator>=(const iterator& rhs) { return !(*this < rhs); }

        private:
            void rebuild_data();

            const block_t* block_;
            item_metadata* metadata_;
            item_data data_;
        };

        class r_iterator {
        public:
            r_iterator(const block_t* block, item_metadata* metadata);

            inline const item_data& operator*() const { return data_; }
            inline const item_data* operator->() const { return &data_; }

            // note: metadata is stored in reverse order, so iterator increment and decrement is reversed
            inline const r_iterator& operator++() {
                metadata_++;
                rebuild_data();
                return *this;
            }
            inline const r_iterator& operator--() {
                metadata_--;
                rebuild_data();
                return *this;
            }
            inline r_iterator operator++(int) {
                auto temp = r_iterator(block_, metadata_);
                metadata_++;
                rebuild_data();
                return temp;
            }
            inline r_iterator operator--(int) {
                auto temp = r_iterator(block_, metadata_);
                metadata_--;
                rebuild_data();
                return temp;
            }
            inline r_iterator operator+(int i) { return r_iterator(block_, metadata_ + i); }
            inline r_iterator operator-(int i) { return r_iterator(block_, metadata_ - i); }
            friend inline r_iterator operator+(int i, const r_iterator& rhs) {
                return r_iterator(rhs.block_, rhs.metadata_ + i);
            }
            friend inline r_iterator operator-(int i, const r_iterator& rhs) {
                return r_iterator(rhs.block_, rhs.metadata_ - i);
            }
            friend int operator-(const r_iterator& lhs, const r_iterator& rhs) {
                assert(lhs.block_ == rhs.block_);
                return lhs.metadata_ - rhs.metadata_;
            }

            inline r_iterator& operator=(const r_iterator& rhs) {
                metadata_ = rhs.metadata_;
                rebuild_data();
                return *this;
            }
            inline r_iterator& operator+=(int rhs) {
                metadata_ += rhs;
                rebuild_data();
                return *this;
            }
            inline r_iterator& operator-=(int rhs) {
                metadata_ -= rhs;
                rebuild_data();
                return *this;
            }

            inline bool operator==(const r_iterator& rhs) { return metadata_ == rhs.metadata_; }
            inline bool operator<(const r_iterator& rhs) { return metadata_ < rhs.metadata_; }
            inline bool operator>(const r_iterator& rhs) { return metadata_ > rhs.metadata_; }
            inline bool operator!=(const r_iterator& rhs) { return !(*this == rhs); }
            inline bool operator<=(const r_iterator& rhs) { return !(*this > rhs); }
            inline bool operator>=(const r_iterator& rhs) { return !(*this < rhs); }

        private:
            void rebuild_data();

            const block_t* block_;
            item_metadata* metadata_;
            item_data data_;
        };

        friend class iterator;
        friend class r_iterator;

        block_t(std::pmr::memory_resource* resource);
        ~block_t();
        block_t(const block_t&) = delete;
        block_t(block_t&&) = default;
        block_t& operator=(const block_t&) = delete;
        block_t& operator=(block_t&&) = default;

        void initialize(size_t size = DEFAULT_BLOCK_SIZE);

        size_t available_memory() const;
        // does not include header
        size_t occupied_memory() const;
        bool is_memory_available(size_t request_size) const;
        bool is_empty() const;
        bool is_valid() const;

        bool append(uint64_t id, const_data_ptr_t append_buffer, size_t buffer_size);
        bool remove(uint64_t id);

        bool contains(uint64_t id) const;
        size_t size_of(uint64_t id) const;
        data_ptr_t data_of(uint64_t id) const;
        std::pair<data_ptr_t, size_t> get_item(uint64_t id) const;

        size_t count() const;
        uint64_t first_id() const;
        uint64_t last_id() const;

        data_ptr_t internal_buffer();
        size_t block_size() const;
        // should be called when internal_buffer is used for reading to restore metadata
        void restore_block();
        void reset();

        // after splits this block will contain the first half
        // best case: second block >= first block
        // worst case: 2 blocks will be created
        [[nodiscard]] std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>>
        split_append(uint64_t id, const_data_ptr_t append_buffer, size_t buffer_size);
        // creates new block and puts last "count" elements there
        [[nodiscard]] std::unique_ptr<block_t> split(size_t count);
        // merge other block to this one
        void merge(std::unique_ptr<block_t> block);

        void recalculate_checksum();
        // return false if block is not in the same state as when check sum was calculated
        bool varify_checksum() const;

        // block is an ordered container, data cannot be modified by iterator
        iterator begin() const { return cbegin(); }
        iterator end() const { return cend(); }
        iterator cbegin() const { return iterator(this, end_ - 1); }
        iterator cend() const { return iterator(this, last_item_metadata_ - 1); }
        r_iterator rbegin() const { return r_iterator({this, last_item_metadata_}); }
        r_iterator rend() const { return r_iterator({this, end_}); }

    private:
        item_metadata* find_item_(uint64_t id) const;
        uint64_t calculate_checksum_() const;

        std::pmr::memory_resource* resource_;
        // The pointer to the internal buffer that will be read or written, including the buffer header
        data_ptr_t internal_buffer_;
        bool is_valid_ = false;

        // start location of free part
        data_ptr_t buffer_;
        // store explicitly to avoid calculating it every time
        item_metadata* end_;
        // where last written id/pointer pair is located
        item_metadata* last_item_metadata_;
        size_t full_size_;
        // The size of free part
        size_t available_memory_;
        size_t* count_;
        uint64_t* checksum_;
    };

    [[nodiscard]] static inline std::unique_ptr<block_t> create_initialize(std::pmr::memory_resource* resource,
                                                                           size_t size = DEFAULT_BLOCK_SIZE) {
        std::unique_ptr<block_t> block = std::make_unique<block_t>(resource);
        block->initialize(size);
        return block;
    }
} // namespace core::b_plus_tree