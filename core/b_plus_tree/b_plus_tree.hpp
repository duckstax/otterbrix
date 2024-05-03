#pragma once

#include "segment_tree.hpp"
#include <atomic>
#include <deque>
#include <filesystem>
#include <queue>
#include <shared_mutex>
#include <string_view>

namespace core::b_plus_tree {

    // current header size of segment_tree supports 2^14 - 1 blocks which is 2^14 - 1 items in worst case
    // max leaf node size <= 16383
    // for a round power of 2:
    // idealy DEFAULT_NODE_CAPACITY and MAX_NODE_CAPACITY % 4 == 0
    static constexpr size_t MAX_NODE_CAPACITY = 8192u;
    static constexpr size_t DEFAULT_NODE_CAPACITY = 128u;
    static constexpr size_t METADATA_SIZE = DEFAULT_BLOCK_SIZE; // will give 2^15 - 1 leaf nodes or 268'435'455 items

    class btree_t {
    public:
        class base_node_t {
        public:
            base_node_t(std::pmr::memory_resource* resource, size_t min_node_capacity, size_t max_node_capacity);
            virtual ~base_node_t() = default;

            virtual bool is_inner_node() const = 0;
            virtual bool is_leaf_node() const = 0;

            void lock_shared();
            void unlock_shared();
            void lock_exclusive();
            void unlock_exclusive();
            virtual size_t count() const = 0;
            virtual size_t unique_entry_count() const = 0;

            virtual base_node_t* find_node(uint64_t id) = 0;
            virtual void balance(base_node_t* neighbour) = 0;
            virtual void merge(base_node_t* neighbour) = 0;

            virtual uint64_t min_id() const = 0;
            virtual uint64_t max_id() const = 0;

            // will be used everywhere
            base_node_t* left_node_ = nullptr;
            base_node_t* right_node_ = nullptr;

        protected:
            std::pmr::memory_resource* resource_;
            std::shared_mutex node_mutex_;
            size_t min_node_capacity_;
            size_t max_node_capacity_;
        };

        class leaf_node_t : public base_node_t {
        public:
            leaf_node_t(std::pmr::memory_resource* resource,
                        std::unique_ptr<core::filesystem::file_handle_t> file,
                        uint64_t segment_tree_id,
                        size_t min_node_capacity,
                        size_t max_node_capacity);
            ~leaf_node_t() = default;

            bool is_inner_node() const override { return false; }
            bool is_leaf_node() const override { return true; }

            base_node_t* find_node(uint64_t) override;
            bool append(uint64_t id, const_data_ptr_t buffer, size_t buffer_size);
            bool remove(uint64_t id, const_data_ptr_t buffer, size_t buffer_size);
            bool remove_id(uint64_t id);
            [[nodiscard]] leaf_node_t* split(std::unique_ptr<core::filesystem::file_handle_t> file,
                                             uint64_t segment_tree_id);
            void balance(base_node_t* neighbour) override;
            void merge(base_node_t* neighbour) override;

            bool contains_id(uint64_t id);
            bool contains(uint64_t id, const_data_ptr_t buffer, size_t buffer_size);
            size_t item_count(uint64_t id);
            std::pair<data_ptr_t, size_t> get_item(uint64_t id, size_t index);
            void get_items(std::vector<std::pair<data_ptr_t, size_t>>& result, uint64_t id);

            uint64_t min_id() const override;
            uint64_t max_id() const override;

            size_t count() const override;
            size_t unique_entry_count() const override;
            uint64_t segment_tree_id() const;
            void flush() const;
            void load();

            segment_tree_t::iterator begin() const { return segment_tree_->begin(); }
            segment_tree_t::iterator end() const { return segment_tree_->end(); }
            segment_tree_t::iterator cbegin() const { return segment_tree_->cbegin(); }
            segment_tree_t::iterator cend() const { return segment_tree_->cend(); }
            segment_tree_t::r_iterator rbegin() const { return segment_tree_->rbegin(); }
            segment_tree_t::r_iterator rend() const { return segment_tree_->rend(); }

        private:
            leaf_node_t(std::pmr::memory_resource* resource,
                        std::unique_ptr<segment_tree_t> segment_tree,
                        uint64_t segment_tree_id,
                        size_t min_node_capacity,
                        size_t max_node_capacity);
            std::unique_ptr<segment_tree_t> segment_tree_;
            uint64_t segment_tree_id_;
        };

        class inner_node_t : public base_node_t {
        public:
            inner_node_t(std::pmr::memory_resource* resource, size_t min_node_capacity, size_t max_node_capacity);
            ~inner_node_t();

            bool is_inner_node() const override { return true; }
            bool is_leaf_node() const override { return false; }

            void initialize(base_node_t* node_1, base_node_t* node_2);
            [[nodiscard]] base_node_t* deinitialize();

            base_node_t* find_node(uint64_t id) override;
            void insert(base_node_t* node);
            void remove(base_node_t* node);
            [[nodiscard]] inner_node_t* split();
            void balance(base_node_t* neighbour) override;
            void merge(base_node_t* neighbour) override;
            void build(base_node_t** nodes, size_t count);

            size_t count() const override;
            size_t unique_entry_count() const override;

            uint64_t min_id() const override;
            uint64_t max_id() const override;

        private:
            base_node_t** nodes_;
            base_node_t** nodes_end_;
        };

        btree_t(std::pmr::memory_resource* resource,
                core::filesystem::local_file_system_t& fs,
                std::string storage_directory,
                size_t max_node_capacity = DEFAULT_NODE_CAPACITY);
        ~btree_t();

        //template<typename T, typename Serializer>
        //bool append(uint64_t id, T item, Serializer serializer);
        bool append(uint64_t id, const_data_ptr_t buffer, size_t buffer_size);
        bool remove(uint64_t id, const_data_ptr_t buffer, size_t buffer_size);
        bool remove_id(uint64_t id);

        template<typename T, typename Deserializer, typename Predicate>
        bool scan_ascending(uint64_t min_id,
                            uint64_t max_id,
                            size_t limit,
                            std::vector<std::pair<uint64_t, T>>* result,
                            Deserializer deserializer,
                            Predicate predicate);
        template<typename T, typename Deserializer, typename Predicate>
        bool scan_decending(uint64_t min_id,
                            uint64_t max_id,
                            size_t limit,
                            std::vector<std::pair<uint64_t, T>>* result,
                            Deserializer deserializer,
                            Predicate predicate);

        void list_ids(std::vector<uint64_t>& result);

        // full flush and load for now
        // TODO: flush and load only modified leaves
        void flush();
        void load();

        bool contains_id(uint64_t id);
        bool contains(uint64_t id, const_data_ptr_t buffer, size_t buffer_size);
        size_t item_count(uint64_t id);
        std::pair<data_ptr_t, size_t> get_item(uint64_t id, size_t index);
        void get_items(std::vector<std::pair<data_ptr_t, size_t>>& result, uint64_t id);
        size_t size() const;

    private:
        leaf_node_t* find_leaf_node_(uint64_t id);
        void release_locks_(std::deque<base_node_t*>& modified_nodes) const;
        uint64_t get_unique_id_();

        core::filesystem::local_file_system_t& fs_;
        std::pmr::memory_resource* resource_;
        std::shared_mutex tree_mutex_;
        base_node_t* root_ = nullptr;
        std::filesystem::path storage_directory_;
        std::string segment_tree_name_ = "segmented_block";

        size_t min_node_capacity_;    // == max / 4
        size_t merge_share_boundary_; // == max / 2
        size_t max_node_capacity_;
        std::atomic<size_t> item_count_{0};
        std::atomic<size_t> leaf_nodes_count_{0};
        std::queue<uint64_t> missed_ids_;
        static constexpr std::string_view metadata_file_name_ = "metadata";
    };

    template<typename T, typename Deserializer, typename Predicate>
    bool btree_t::scan_ascending(uint64_t min_id,
                                 uint64_t max_id,
                                 size_t limit,
                                 std::vector<std::pair<uint64_t, T>>* result,
                                 Deserializer deserializer,
                                 Predicate predicate) {
        auto first_leaf = find_leaf_node_(min_id);
        if (!first_leaf || limit == 0) {
            return false;
        }

        tree_mutex_.lock_shared();
        first_leaf->unlock_shared();

        while (first_leaf) {
            if (first_leaf->min_id() > max_id) {
                break;
            }

            for (auto block = first_leaf->begin(); block != first_leaf->end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    if (it->id > max_id) {
                        tree_mutex_.unlock_shared();
                        return true;
                    } else if (it->id < min_id) {
                        continue;
                    }
                    for (auto item = it->items.begin(); item != it->items.end(); item++) {
                        T t = deserializer(reinterpret_cast<void*>(item->first), item->second);
                        if (predicate(t)) {
                            result->emplace_back(it->id, std::move(t));
                            limit--;
                            if (limit == 0) {
                                tree_mutex_.unlock_shared();
                                return true;
                            }
                        }
                    }
                }
            }
            first_leaf = static_cast<leaf_node_t*>(first_leaf->right_node_);
        }

        tree_mutex_.unlock_shared();
        return true;
    }

    template<typename T, typename Deserializer, typename Predicate>
    bool btree_t::scan_decending(uint64_t min_id,
                                 uint64_t max_id,
                                 size_t limit,
                                 std::vector<std::pair<uint64_t, T>>* result,
                                 Deserializer deserializer,
                                 Predicate predicate) {
        auto last_leaf = find_leaf_node_(max_id);
        if (!last_leaf || limit == 0) {
            return false;
        }

        tree_mutex_.lock_shared();
        last_leaf->unlock_shared();

        while (last_leaf) {
            if (last_leaf->max_id() < min_id) {
                break;
            }

            for (auto block = last_leaf->rbegin(); block != last_leaf->rend(); block++) {
                for (auto it = block->rbegin(); it != block->rend(); it++) {
                    if (it->id < min_id) {
                        tree_mutex_.unlock_shared();
                        return true;
                    } else if (it->id > max_id) {
                        continue;
                    }
                    for (auto item = it->items.rbegin(); item != it->items.rend(); item++) {
                        T t = deserializer(reinterpret_cast<void*>(item->first), item->second);
                        if (predicate(t)) {
                            result->emplace_back(it->id, std::move(t));
                            limit--;
                            if (limit == 0) {
                                tree_mutex_.unlock_shared();
                                return true;
                            }
                        }
                    }
                }
            }
            last_leaf = static_cast<leaf_node_t*>(last_leaf->left_node_);
        }

        tree_mutex_.unlock_shared();
        return true;
    }

} // namespace core::b_plus_tree