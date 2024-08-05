#include "b_plus_tree.hpp"

#include <algorithm>
#include <cstring>

using file_flags = core::filesystem::file_flags;

namespace core::b_plus_tree {

    /* base node */

    btree_t::base_node_t::base_node_t(std::pmr::memory_resource* resource,
                                      size_t min_node_capacity,
                                      size_t max_node_capacity)
        : resource_(resource)
        , min_node_capacity_(min_node_capacity)
        , max_node_capacity_(max_node_capacity) {}

    void btree_t::base_node_t::lock_shared() { node_mutex_.lock_shared(); }

    void btree_t::base_node_t::unlock_shared() { node_mutex_.unlock_shared(); }

    void btree_t::base_node_t::lock_exclusive() { node_mutex_.lock(); }

    void btree_t::base_node_t::unlock_exclusive() { node_mutex_.unlock(); }

    /* inner node */

    btree_t::inner_node_t::inner_node_t(std::pmr::memory_resource* resource,
                                        size_t min_node_capacity,
                                        size_t max_node_capacity)
        : btree_t::base_node_t(resource, min_node_capacity, max_node_capacity) {
        nodes_ =
            static_cast<btree_t::base_node_t**>(resource_->allocate(sizeof(btree_t::base_node_t*) * max_node_capacity));
        nodes_end_ = nodes_;
    }

    btree_t::inner_node_t::~inner_node_t() {
        for (auto it = nodes_; it < nodes_end_; it++) {
            delete *it;
        }
        resource_->deallocate(nodes_, sizeof(btree_t::base_node_t*) * max_node_capacity_);
    }

    void btree_t::inner_node_t::initialize(base_node_t* node_1, base_node_t* node_2) {
        assert(nodes_ == nodes_end_ && "already initialized");

        *nodes_ = node_1;
        *(nodes_ + 1) = node_2;
        nodes_end_ += 2;

        node_1->right_node_ = node_2;
        node_2->left_node_ = node_1;
    }

    btree_t::base_node_t* btree_t::inner_node_t::deinitialize() {
        assert(count() == 1 && "cannot deinitialize valid node");
        nodes_end_ = nodes_; // any pointers still stored wont be destroyed
        return *nodes_;
    }

    btree_t::base_node_t* btree_t::inner_node_t::find_node(uint64_t id) {
        assert(count() > 0 && "inner node with 0 items does not suppose to exist");
        auto it =
            std::lower_bound(nodes_, nodes_end_, id, [](base_node_t* n, uint64_t id) { return n->min_id() < id; });
        // some edge cases around begin and end
        if (it == nodes_end_) {
            return *(--it);
        } else if (it != nodes_) {
            return ((*it)->min_id() > id) ? *(--it) : *it;
        }
        return *it;
    }

    void btree_t::inner_node_t::insert(base_node_t* node) {
        assert(count() > 1 &&
               "cannot insert key/node pair in inner block with less then 2 items inside. use initialize method");
        assert(count() < max_node_capacity_);

        uint64_t id = node->min_id();
        base_node_t** it =
            std::lower_bound(nodes_, nodes_end_, id, [](base_node_t* n, uint64_t id) { return n->min_id() < id; });
        size_t move_count = nodes_end_ - it;
        size_t pos = it - nodes_;
        std::memmove(nodes_ + pos + 1, nodes_ + pos, move_count * sizeof(base_node_t*));
        *it = node;

        // since insert into empty inner_node is not possible, inserted node will have neighbour to the left or right
        if (it != nodes_end_) {
            if ((*(it + 1))->left_node_) {
                node->left_node_ = (*(it + 1))->left_node_;
                node->left_node_->right_node_ = node;
            }
            node->right_node_ = *(it + 1);
            (*(it + 1))->left_node_ = node;
        } else {
            if ((*(it - 1))->right_node_) {
                node->right_node_ = (*(it - 1))->right_node_;
                node->right_node_->left_node_ = node;
            }
            node->left_node_ = *(it - 1);
            (*(it - 1))->right_node_ = node;
        }
        nodes_end_++;
    }

    void btree_t::inner_node_t::remove(base_node_t* node) {
        base_node_t** it = std::find_if(nodes_, nodes_end_, [&node](base_node_t* n) { return n == node; });

        assert(it != nodes_end_ && "node is not present");
        std::memmove(it, it + 1, (nodes_end_ - it - 1) * sizeof(base_node_t*));
        nodes_end_--;
        if (node->left_node_) {
            node->left_node_->right_node_ = (node->right_node_) ? node->right_node_ : nullptr;
        }
        if (node->right_node_) {
            node->right_node_->left_node_ = (node->left_node_) ? node->left_node_ : nullptr;
        }
        node->unlock_exclusive();
        delete node;
    }

    btree_t::inner_node_t* btree_t::inner_node_t::split() {
        assert(count() > 1);
        inner_node_t* splited_node = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
        size_t split_size = count() / 2;
        std::memcpy(splited_node->nodes_, nodes_ + count() - split_size, split_size * sizeof(base_node_t*));
        nodes_end_ -= split_size;
        splited_node->nodes_end_ += split_size;

        return splited_node;
    }

    void btree_t::inner_node_t::balance(base_node_t* neighbour) {
        assert(left_node_ == neighbour || right_node_ == neighbour && "balance_node requires neighbouring nodes");
        assert(min_id() > neighbour->max_id() || max_id() < neighbour->min_id());
        // easier to check it where it is needed, then to add 2 new cases for it
        assert(count() < neighbour->count());

        inner_node_t* other = static_cast<inner_node_t*>(neighbour);

        size_t rebalance_size = (count() + other->count()) / 2 - count();
        if (min_id() > other->max_id()) {
            std::memmove(nodes_ + rebalance_size, nodes_, count() * sizeof(base_node_t*));
            std::memmove(nodes_, (other->nodes_end_ - rebalance_size), rebalance_size * sizeof(base_node_t*));
        } else {
            std::memmove(nodes_end_, other->nodes_, rebalance_size * sizeof(base_node_t*));
            std::memmove(other->nodes_,
                         other->nodes_ + rebalance_size,
                         (other->count() - rebalance_size) * sizeof(base_node_t*));
        }
        nodes_end_ += rebalance_size;
        other->nodes_end_ -= rebalance_size;
    }

    void btree_t::inner_node_t::merge(base_node_t* neighbour) {
        assert(left_node_ == neighbour || right_node_ == neighbour && "merge requires neighbouring nodes");
        assert(min_id() > neighbour->max_id() || max_id() < neighbour->min_id());
        assert(count() != 0 && neighbour->count() != 0);

        inner_node_t* other = static_cast<inner_node_t*>(neighbour);

        size_t delta_count = other->count();
        if (min_id() > other->max_id()) {
            std::memmove(nodes_ + delta_count, nodes_, count() * sizeof(base_node_t*));
            std::memmove(nodes_, other->nodes_, delta_count * sizeof(base_node_t*));
        } else {
            std::memmove(nodes_end_, other->nodes_, delta_count * sizeof(base_node_t*));
        }
        nodes_end_ += delta_count;
        other->nodes_end_ -= delta_count;
    }

    void btree_t::inner_node_t::build(btree_t::inner_node_t::base_node_t** nodes, size_t count) {
        assert(count <= max_node_capacity_);
        std::memcpy(nodes_, nodes, count * sizeof(base_node_t*));
        nodes_end_ = nodes_ + count;
    }

    size_t btree_t::inner_node_t::count() const { return nodes_end_ - nodes_; }

    size_t btree_t::inner_node_t::unique_entry_count() const { return count(); }

    uint64_t btree_t::inner_node_t::min_id() const { return (nodes_ != nodes_end_) ? (*nodes_)->min_id() : 0; }

    uint64_t btree_t::inner_node_t::max_id() const {
        return (nodes_ != nodes_end_) ? (*(nodes_end_ - 1))->max_id() : INVALID_ID;
    }

    /* leaf node */

    btree_t::leaf_node_t::leaf_node_t(std::pmr::memory_resource* resource,
                                      std::unique_ptr<core::filesystem::file_handle_t> file,
                                      uint64_t segment_tree_id,
                                      size_t min_node_capacity,
                                      size_t max_node_capacity)
        : btree_t::base_node_t(resource, min_node_capacity, max_node_capacity)
        , segment_tree_(std::make_unique<segment_tree_t>(resource, std::move(file)))
        , segment_tree_id_(segment_tree_id) {}

    btree_t::leaf_node_t::leaf_node_t(std::pmr::memory_resource* resource,
                                      std::unique_ptr<segment_tree_t> segment_tree,
                                      uint64_t segment_tree_id,
                                      size_t min_node_capacity,
                                      size_t max_node_capacity)
        : btree_t::base_node_t(resource, min_node_capacity, max_node_capacity)
        , segment_tree_(std::move(segment_tree))
        , segment_tree_id_(segment_tree_id) {}

    btree_t::base_node_t* btree_t::leaf_node_t::find_node(uint64_t) { return this; }

    bool btree_t::leaf_node_t::append(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        return segment_tree_->append(id, buffer, buffer_size);
    }
    bool btree_t::leaf_node_t::remove(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        return segment_tree_->remove(id, buffer, buffer_size);
    }
    bool btree_t::leaf_node_t::remove_id(uint64_t id) { return segment_tree_->remove_id(id); }

    btree_t::leaf_node_t* btree_t::leaf_node_t::split(std::unique_ptr<core::filesystem::file_handle_t> file,
                                                      uint64_t segment_tree_id) {
        return new btree_t::leaf_node_t(resource_,
                                        segment_tree_->split(std::move(file)),
                                        segment_tree_id,
                                        min_node_capacity_,
                                        max_node_capacity_);
    }

    void btree_t::leaf_node_t::balance(base_node_t* neighbour) {
        assert(left_node_ == neighbour || right_node_ == neighbour && "balance_node requires neighbouring nodes");
        if (unique_entry_count() > neighbour->unique_entry_count()) {
            static_cast<leaf_node_t*>(neighbour)->segment_tree_->balance_with(segment_tree_);
        } else {
            segment_tree_->balance_with(static_cast<leaf_node_t*>(neighbour)->segment_tree_);
        }
    }

    void btree_t::leaf_node_t::merge(base_node_t* neighbour) {
        assert(left_node_ == neighbour || right_node_ == neighbour && "merge requires neighbouring nodes");
        segment_tree_->merge(static_cast<leaf_node_t*>(neighbour)->segment_tree_);
    }

    bool btree_t::leaf_node_t::contains_id(uint64_t id) { return segment_tree_->contains_id(id); }
    bool btree_t::leaf_node_t::contains(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        return segment_tree_->contains(id, buffer, buffer_size);
    }
    size_t btree_t::leaf_node_t::item_count(uint64_t id) { return segment_tree_->item_count(id); }
    std::pair<data_ptr_t, size_t> btree_t::leaf_node_t::get_item(uint64_t id, size_t index) {
        return segment_tree_->get_item(id, index);
    }
    void btree_t::leaf_node_t::get_items(std::vector<std::pair<data_ptr_t, size_t>>& result, uint64_t id) {
        segment_tree_->get_items(result, id);
    }
    uint64_t btree_t::leaf_node_t::min_id() const { return segment_tree_->min_id(); }
    uint64_t btree_t::leaf_node_t::max_id() const { return segment_tree_->max_id(); }
    size_t btree_t::leaf_node_t::count() const { return segment_tree_->count(); }
    size_t btree_t::leaf_node_t::unique_entry_count() const { return segment_tree_->unique_id_count(); }
    uint64_t btree_t::leaf_node_t::segment_tree_id() const { return segment_tree_id_; }
    void btree_t::leaf_node_t::flush() const { segment_tree_->flush(); }
    void btree_t::leaf_node_t::load() { segment_tree_->lazy_load(); }

    /* btree */

    btree_t::btree_t(std::pmr::memory_resource* resource,
                     core::filesystem::local_file_system_t& fs,
                     std::string storage_directory,
                     size_t max_node_capacity)
        : fs_(fs)
        , resource_(resource)
        , storage_directory_(storage_directory)
        , min_node_capacity_(max_node_capacity / 4)
        , merge_share_boundary_(max_node_capacity / 2)
        , max_node_capacity_(max_node_capacity) {
        assert(max_node_capacity < MAX_NODE_CAPACITY);
        if (!directory_exists(fs_, storage_directory_)) {
            create_directory(fs_, storage_directory_);
        }
    }

    btree_t::~btree_t() {
        if (root_) {
            delete root_;
        }
    }

    bool btree_t::append(uint64_t id, const_data_ptr_t buffer, uint64_t buffer_size) {
        tree_mutex_.lock(); // needed for root check
        if (root_ == nullptr) {
            uint64_t segment_tree_id = get_unique_id_();
            std::filesystem::path file_name = storage_directory_;
            file_name /= std::filesystem::path(segment_tree_name_ + std::to_string(segment_tree_id));
            std::unique_ptr<core::filesystem::file_handle_t> file =
                open_file(fs_, file_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
            root_ = static_cast<base_node_t*>(
                new leaf_node_t(resource_, std::move(file), segment_tree_id, min_node_capacity_, max_node_capacity_));
            reinterpret_cast<leaf_node_t*>(root_)->append(id, buffer, buffer_size);
            leaf_nodes_count_++;
            item_count_++;
            tree_mutex_.unlock();
            return true;
        } else if (root_->is_leaf_node()) {
            assert(root_->unique_entry_count() != 0);
            bool result;
            if (root_->unique_entry_count() < max_node_capacity_) {
                result = static_cast<leaf_node_t*>(root_)->append(id, buffer, buffer_size);
            } else {
                uint64_t segment_tree_id = get_unique_id_();
                std::filesystem::path file_name = storage_directory_;
                file_name /= std::filesystem::path(segment_tree_name_ + std::to_string(segment_tree_id));
                std::unique_ptr<core::filesystem::file_handle_t> file =
                    open_file(fs_, file_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
                leaf_node_t* splited_node = static_cast<leaf_node_t*>(root_)->split(std::move(file), segment_tree_id);
                leaf_nodes_count_++;
                if (splited_node->min_id() < id) {
                    result = splited_node->append(id, buffer, buffer_size);
                } else {
                    result = static_cast<leaf_node_t*>(root_)->append(id, buffer, buffer_size);
                }
                inner_node_t* new_root = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
                new_root->initialize(root_, splited_node);
                root_ = static_cast<base_node_t*>(new_root);
            }
            if (result) {
                item_count_++;
            }
            tree_mutex_.unlock();
            return result;
        }

        root_->lock_exclusive(); // before releasing tree mutex, lock root
        base_node_t* current_node = root_;
        base_node_t* parent_node = nullptr;
        std::deque<base_node_t*> modified_nodes;
        bool record_nodes = current_node->unique_entry_count() == max_node_capacity_;
        // traversing down and maintaining a stack of pointers
        while (current_node->is_inner_node()) {
            if (current_node->unique_entry_count() < max_node_capacity_) {
                // if there are any marked nodes, they won't be affected by changes to that one. clear modified_nodes stack
                release_locks_(modified_nodes);
                record_nodes = false;
            }
            parent_node = current_node;
            current_node = current_node->find_node(id);
            current_node->lock_exclusive();

            record_nodes = record_nodes || current_node->unique_entry_count() == max_node_capacity_;
            modified_nodes.push_back(parent_node);
            if (!record_nodes) {
                release_locks_(modified_nodes);
            }
        }

        if (!record_nodes || modified_nodes.front()->unique_entry_count() != max_node_capacity_ ||
            current_node->unique_entry_count() < max_node_capacity_) {
            tree_mutex_.unlock();
        }

        bool result;
        if (current_node->unique_entry_count() < max_node_capacity_) {
            // safely append item, modified_nodes will not be affected
            release_locks_(modified_nodes);
            result = static_cast<leaf_node_t*>(current_node)->append(id, buffer, buffer_size);
        } else {
            // append to this node will require node split, which may cause appends and splits inside modified_nodes
            uint64_t segment_tree_id = get_unique_id_();
            std::filesystem::path file_name = storage_directory_;
            file_name /= std::filesystem::path(segment_tree_name_ + std::to_string(segment_tree_id));
            std::unique_ptr<core::filesystem::file_handle_t> file =
                open_file(fs_, file_name, file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE);
            leaf_node_t* splited_node =
                static_cast<leaf_node_t*>(current_node)->split(std::move(file), segment_tree_id);
            leaf_nodes_count_++;

            if (splited_node->min_id() <= id) {
                result = splited_node->append(id, buffer, buffer_size);
            } else {
                result = static_cast<leaf_node_t*>(current_node)->append(id, buffer, buffer_size);
            }

            base_node_t* insert_node = static_cast<base_node_t*>(splited_node);
            inner_node_t* node = nullptr;
            // all nodes in modified_nodes list (exept first one) are full, split each of them, insert splited one in node above
            while (!modified_nodes.empty()) {
                node = static_cast<inner_node_t*>(modified_nodes.back());
                modified_nodes.pop_back();
                if (node->unique_entry_count() < max_node_capacity_) {
                    static_cast<inner_node_t*>(node)->insert(insert_node);
                    insert_node = nullptr;
                } else {
                    base_node_t* splited_upper_node = node->split();

                    if (splited_upper_node->min_id() < insert_node->min_id()) {
                        static_cast<inner_node_t*>(splited_upper_node)->insert(insert_node);
                    } else {
                        node->insert(insert_node);
                    }
                    insert_node = splited_upper_node;
                }
                if (!modified_nodes.empty()) {
                    node->unlock_exclusive();
                }
            }

            if (insert_node && node == root_) {
                // this is above the actual root
                inner_node_t* new_root = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
                new_root->initialize(root_, insert_node);
                root_ = static_cast<base_node_t*>(new_root);
                tree_mutex_.unlock();
            }
            node->unlock_exclusive();
        }
        current_node->unlock_exclusive();
        if (result) {
            item_count_++;
        }
        return result;
    }

    bool btree_t::remove(uint64_t id, const_data_ptr_t buffer, uint64_t buffer_size) {
        tree_mutex_.lock(); // needed for root check
        if (root_ == nullptr) {
            tree_mutex_.unlock();
            return false;
        } else if (root_->is_leaf_node()) {
            bool result = static_cast<leaf_node_t*>(root_)->remove(id, buffer, buffer_size);
            if (result) {
                item_count_--;
            }
            if (root_->count() == 0) {
                missed_ids_.push(static_cast<leaf_node_t*>(root_)->segment_tree_id());
                delete root_;
                root_ = nullptr;
                leaf_nodes_count_--;
            }
            tree_mutex_.unlock();
            return result;
        }

        root_->lock_exclusive(); // before releasing tree mutex, lock root
        base_node_t* current_node = root_;
        base_node_t* parent_node = nullptr;
        std::deque<base_node_t*> modified_nodes;
        bool record_nodes = current_node->unique_entry_count() == 1;
        // traversing down and maintaining a stack of pointers
        while (current_node->is_inner_node()) {
            if (current_node->unique_entry_count() > min_node_capacity_) {
                // if there are any marked nodes, they won't be affected by changes to that one. clear modified_nodes stack
                release_locks_(modified_nodes);
                record_nodes = false;
            }
            parent_node = current_node;
            current_node = current_node->find_node(id);
            current_node->lock_exclusive();

            record_nodes = record_nodes || (current_node->unique_entry_count() == min_node_capacity_ &&
                                            current_node != root_); // root does not obey to minimum requirements
            modified_nodes.push_back(parent_node);
            if (!record_nodes) {
                release_locks_(modified_nodes);
            }
        }

        if (!static_cast<leaf_node_t*>(current_node)->contains_id(id)) {
            tree_mutex_.unlock();
            release_locks_(modified_nodes);
            return false;
        }

        bool result;
        if (current_node->unique_entry_count() > min_node_capacity_) {
            // safely remove item, modified_nodes will not be affected
            release_locks_(modified_nodes);
            result = static_cast<leaf_node_t*>(current_node)->remove(id, buffer, buffer_size);
            tree_mutex_.unlock();
        } else {
            // merge into current node can only be performed within parent node
            // but merging current node into neighbour can be done anytime
            // share could be done with any neighbour
            // merge puts node further from lower and upper rebalancing point, so it is preferable
            // TODO: do some test to check if it is the right approach or "first share then merge" approach will be faster

            assert(current_node->left_node_ || current_node->right_node_ && "not a root node has no neighbours");
            // guaranteed that at least one neighbour exist

            result = static_cast<leaf_node_t*>(current_node)->remove(id, buffer, buffer_size);
            if (current_node->unique_entry_count() > min_node_capacity_) {
                // safely remove item, modified_nodes will not be affected
                release_locks_(modified_nodes);
                result = static_cast<leaf_node_t*>(current_node)->remove(id, buffer, buffer_size);
                tree_mutex_.unlock();
            }

            if (!modified_nodes.empty() && result) {
                modified_nodes.pop_back(); // remove parent node, since it is already aquired
            }
            // TODO: rework recursive node removal to be more friendly with multithreading
            while (parent_node && result) {
                if (current_node->right_node_ &&
                    current_node->right_node_->unique_entry_count() <= merge_share_boundary_) {
                    current_node->right_node_->lock_exclusive();
                    current_node->right_node_->merge(current_node);
                    current_node->right_node_->unlock_exclusive();
                } else if (current_node->left_node_ && current_node->left_node_->count() <= merge_share_boundary_) {
                    current_node->left_node_->lock_exclusive();
                    current_node->left_node_->merge(current_node);
                    current_node->left_node_->unlock_exclusive();
                } else {
                    // cannot merge with anyone
                    if (current_node->right_node_) {
                        current_node->right_node_->lock_exclusive();
                        current_node->balance(current_node->right_node_);
                        current_node->right_node_->unlock_exclusive();
                    } else {
                        current_node->left_node_->lock_exclusive();
                        current_node->balance(current_node->left_node_);
                        current_node->left_node_->unlock_exclusive();
                    }
                    // amount of nodes did not change, so there is no need to check modified_nodes
                    release_locks_(modified_nodes);
                    parent_node->unlock_exclusive();
                    break;
                }

                if (current_node->is_leaf_node()) {
                    missed_ids_.push(static_cast<leaf_node_t*>(current_node)->segment_tree_id());
                    leaf_nodes_count_--;
                }
                static_cast<inner_node_t*>(parent_node)->remove(current_node);
                if (parent_node->unique_entry_count() == 1) {
                    // parent is a root node
                    base_node_t* new_root = static_cast<inner_node_t*>(parent_node)->deinitialize();
                    delete parent_node;
                    root_ = new_root;
                    break;
                }

                if (modified_nodes.empty()) {
                    parent_node->unlock_exclusive();
                    break;
                }

                current_node->unlock_exclusive();
                current_node = parent_node;
                if (!modified_nodes.empty()) {
                    parent_node = modified_nodes.back();
                    modified_nodes.pop_back();
                } else {
                    parent_node = nullptr;
                }
            }
            tree_mutex_.unlock();
        }

        current_node->unlock_exclusive();
        if (result) {
            item_count_--;
        }
        return result;
    }

    bool btree_t::remove_id(uint64_t id) {
        tree_mutex_.lock(); // needed for root check
        if (root_ == nullptr) {
            tree_mutex_.unlock();
            return false;
        } else if (root_->is_leaf_node()) {
            size_t count_delta = static_cast<leaf_node_t*>(root_)->item_count(id);
            bool result = static_cast<leaf_node_t*>(root_)->remove_id(id);
            if (result) {
                item_count_ -= count_delta;
            }
            if (root_->count() == 0) {
                missed_ids_.push(static_cast<leaf_node_t*>(root_)->segment_tree_id());
                delete root_;
                root_ = nullptr;
                leaf_nodes_count_ -= count_delta;
            }
            tree_mutex_.unlock();
            return result;
        }

        root_->lock_exclusive(); // before releasing tree mutex, lock root
        base_node_t* current_node = root_;
        base_node_t* parent_node = nullptr;
        std::deque<base_node_t*> modified_nodes;
        bool record_nodes = current_node->unique_entry_count() == 1;
        // traversing down and maintaining a stack of pointers
        while (current_node->is_inner_node()) {
            if (current_node->unique_entry_count() > min_node_capacity_) {
                // if there are any marked nodes, they won't be affected by changes to that one. clear modified_nodes stack
                release_locks_(modified_nodes);
                record_nodes = false;
            }
            parent_node = current_node;
            current_node = current_node->find_node(id);
            current_node->lock_exclusive();

            record_nodes = record_nodes || (current_node->unique_entry_count() == min_node_capacity_ &&
                                            current_node != root_); // root does not obey to minimum requirements
            modified_nodes.push_back(parent_node);
            if (!record_nodes) {
                release_locks_(modified_nodes);
            }
        }

        if (!static_cast<leaf_node_t*>(current_node)->contains_id(id)) {
            tree_mutex_.unlock();
            release_locks_(modified_nodes);
            return false;
        }

        bool result;
        size_t count_delta = static_cast<leaf_node_t*>(current_node)->item_count(id);
        if (current_node->unique_entry_count() > min_node_capacity_) {
            // safely remove item, modified_nodes will not be affected
            release_locks_(modified_nodes);
            result = static_cast<leaf_node_t*>(current_node)->remove_id(id);
            tree_mutex_.unlock();
        } else {
            // merge into current node can only be performed within parent node
            // but merging current node into neighbour can be done anytime
            // share could be done with any neighbour
            // merge puts node further from lower and upper rebalancing point, so it is preferable
            // TODO: do some test to check if it is the right approach or "first share then merge" approach will be faster

            assert(current_node->left_node_ || current_node->right_node_ && "not a root node has no neighbours");
            // guaranteed that at least one neighbour exist

            result = static_cast<leaf_node_t*>(current_node)->remove_id(id);

            if (!modified_nodes.empty() && result) {
                modified_nodes.pop_back(); // remove parent node, since it is already aquired
            }
            // TODO: rework recursive node removal to be more friendly with multithreading
            while (parent_node && result) {
                if (current_node->right_node_ &&
                    current_node->right_node_->unique_entry_count() <= merge_share_boundary_) {
                    current_node->right_node_->lock_exclusive();
                    current_node->right_node_->merge(current_node);
                    current_node->right_node_->unlock_exclusive();
                } else if (current_node->left_node_ &&
                           current_node->left_node_->unique_entry_count() <= merge_share_boundary_) {
                    current_node->left_node_->lock_exclusive();
                    current_node->left_node_->merge(current_node);
                    current_node->left_node_->unlock_exclusive();
                } else {
                    // cannot merge with anyone
                    if (current_node->right_node_) {
                        current_node->right_node_->lock_exclusive();
                        current_node->balance(current_node->right_node_);
                        current_node->right_node_->unlock_exclusive();
                    } else {
                        current_node->left_node_->lock_exclusive();
                        current_node->balance(current_node->left_node_);
                        current_node->left_node_->unlock_exclusive();
                    }
                    // amount of nodes did not change, so there is no need to check modified_nodes
                    release_locks_(modified_nodes);
                    parent_node->unlock_exclusive();
                    break;
                }

                if (current_node->is_leaf_node()) {
                    missed_ids_.push(static_cast<leaf_node_t*>(current_node)->segment_tree_id());
                    leaf_nodes_count_--;
                }
                static_cast<inner_node_t*>(parent_node)->remove(current_node);
                if (parent_node->unique_entry_count() == 1) {
                    // parent is a root node
                    base_node_t* new_root = static_cast<inner_node_t*>(parent_node)->deinitialize();
                    delete parent_node;
                    root_ = new_root;
                    break;
                }

                if (modified_nodes.empty()) {
                    parent_node->unlock_exclusive();
                    break;
                }

                current_node->unlock_exclusive();
                current_node = parent_node;
                if (!modified_nodes.empty()) {
                    parent_node = modified_nodes.back();
                    modified_nodes.pop_back();
                } else {
                    parent_node = nullptr;
                }
            }
            tree_mutex_.unlock();
        }

        current_node->unlock_exclusive();
        if (result) {
            item_count_ -= count_delta;
        }
        return result;
    }

    void btree_t::list_ids(std::vector<uint64_t>& result) {
        auto first_leaf = find_leaf_node_(0);
        if (!first_leaf) {
            return;
        }

        tree_mutex_.lock_shared();
        first_leaf->unlock_shared();

        result.reserve(item_count_);
        while (first_leaf) {
            for (auto block = first_leaf->begin(); block != first_leaf->end(); block++) {
                for (auto it = block->begin(); it != block->end(); it++) {
                    result.push_back(it->id);
                }
            }
            first_leaf = static_cast<leaf_node_t*>(first_leaf->right_node_);
        }

        tree_mutex_.unlock_shared();
    }

    void btree_t::flush() {
        if (leaf_nodes_count_ == 0) {
            return;
        }

        tree_mutex_.lock();

        // got root mutex, no need to lock nodes or save parent node
        base_node_t* current_node = root_;
        while (current_node->is_inner_node()) {
            current_node = static_cast<inner_node_t*>(current_node)->find_node(0);
        }

        leaf_node_t* first_leaf = static_cast<leaf_node_t*>(current_node);
        leaf_node_t* node = first_leaf;

        size_t* buffer = static_cast<size_t*>(resource_->allocate(METADATA_SIZE));
        *buffer = item_count_;
        *(buffer + 1) = leaf_nodes_count_;
        uint64_t* buffer_writer = static_cast<size_t*>(buffer + 2);

        size_t i = 0;
        // save each segment tree
        while (node) {
            node->flush();
            *buffer_writer = node->segment_tree_id();
            buffer_writer++;
            node = static_cast<leaf_node_t*>(node->right_node_);
            i++;
        }

        std::filesystem::path file_name = storage_directory_;
        file_name /= std::filesystem::path(metadata_file_name_);
        std::unique_ptr<core::filesystem::file_handle_t> file =
            open_file(fs_, file_name, file_flags::WRITE | file_flags::FILE_CREATE);
        file->write(static_cast<void*>(buffer), METADATA_SIZE, 0);

        resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
        tree_mutex_.unlock();
    }

    void btree_t::load() {
        tree_mutex_.lock();
        if (root_) {
            delete root_;
            root_ = nullptr;
        }
        std::filesystem::path file_name = storage_directory_;
        file_name /= std::filesystem::path(metadata_file_name_);
        assert(file_exists(fs_, file_name));
        std::unique_ptr<core::filesystem::file_handle_t> file = open_file(fs_, file_name, file_flags::READ);
        size_t* buffer = static_cast<size_t*>(resource_->allocate(METADATA_SIZE));
        file->read(static_cast<void*>(buffer), METADATA_SIZE, 0);

        item_count_ = *buffer;
        leaf_nodes_count_ = *(buffer + 1);
        uint64_t* buffer_reader = static_cast<uint64_t*>(buffer + 2);

        // with some id manipulations, all could be done in one layer
        base_node_t** nodes_layer =
            static_cast<base_node_t**>(resource_->allocate(leaf_nodes_count_ * sizeof(base_node_t*)));
        base_node_t* left_node = nullptr;
        std::vector<uint64_t> ids_; // will be used to restore missing_ids_
        ids_.reserve(leaf_nodes_count_);
        for (size_t i = 0; i < leaf_nodes_count_; i++) {
            uint64_t segment_tree_id = *buffer_reader;
            buffer_reader++;
            ids_.push_back(segment_tree_id);
            std::filesystem::path leaf_file_name = storage_directory_;
            leaf_file_name /= std::filesystem::path(segment_tree_name_ + std::to_string(segment_tree_id));
            assert(file_exists(fs_, leaf_file_name));
            std::unique_ptr<core::filesystem::file_handle_t> leaf_file =
                open_file(fs_, leaf_file_name, file_flags::READ | file_flags::WRITE);
            base_node_t* node = static_cast<base_node_t*>(new leaf_node_t(resource_,
                                                                          std::move(leaf_file),
                                                                          segment_tree_id,
                                                                          min_node_capacity_,
                                                                          max_node_capacity_));

            static_cast<leaf_node_t*>(node)->load();
            *(nodes_layer + i) = node;
            if (left_node) {
                left_node->right_node_ = node;
                node->left_node_ = left_node;
            }
            left_node = node;
        }
        size_t inner_node_pack_size = (max_node_capacity_ + min_node_capacity_) / 2;

        size_t upper_layer_index = 0;
        size_t layer_index = 0;
        size_t layer_count = leaf_nodes_count_;
        left_node = nullptr;
        while (layer_count > 1) {
            while (layer_index < layer_count) {
                inner_node_t* node = new inner_node_t(resource_, min_node_capacity_, max_node_capacity_);
                // check if after creating an upper node there would be enough left for the next one
                if (layer_count - layer_index >= inner_node_pack_size + min_node_capacity_) {
                    node->build(nodes_layer + layer_index, inner_node_pack_size);
                    layer_index += inner_node_pack_size;
                } else {
                    node->build(nodes_layer + layer_index, layer_count - layer_index);
                    layer_index = layer_count;
                }

                if (left_node) {
                    left_node->right_node_ = static_cast<base_node_t*>(node);
                    node->left_node_ = left_node;
                }
                left_node = static_cast<base_node_t*>(node);
                *(nodes_layer + upper_layer_index) = node;
                upper_layer_index++;
            }
            layer_count = upper_layer_index;
            upper_layer_index = 0;
            layer_index = 0;
            left_node = nullptr;
        }

        root_ = *nodes_layer;

        resource_->deallocate(static_cast<void*>(buffer), METADATA_SIZE);
        resource_->deallocate(static_cast<void*>(nodes_layer), leaf_nodes_count_ * sizeof(base_node_t*));
        tree_mutex_.unlock();
    }

    bool btree_t::contains_id(uint64_t id) {
        if (root_ == nullptr) {
            return false;
        }

        auto node = find_leaf_node_(id);
        bool result = false;
        if (node) {
            result = node->contains_id(id);
            node->unlock_shared();
        }
        return result;
    }

    bool btree_t::contains(uint64_t id, const_data_ptr_t buffer, size_t buffer_size) {
        if (root_ == nullptr) {
            return false;
        }

        auto node = find_leaf_node_(id);
        bool result = false;
        if (node) {
            result = node->contains(id, buffer, buffer_size);
            node->unlock_shared();
        }
        return result;
    }

    size_t btree_t::item_count(uint64_t id) {
        if (root_ == nullptr) {
            return 0;
        }

        auto node = find_leaf_node_(id);
        size_t result = 0;
        if (node) {
            result = node->item_count(id);
            node->unlock_shared();
        }
        return result;
    }

    std::pair<data_ptr_t, size_t> btree_t::get_item(uint64_t id, size_t index) {
        if (root_ == nullptr) {
            return {nullptr, 0};
        }

        auto node = find_leaf_node_(id);
        std::pair<data_ptr_t, size_t> result = {nullptr, 0};
        if (node) {
            result = node->get_item(id, index);
            node->unlock_shared();
        }
        return result;
    }

    void btree_t::get_items(std::vector<std::pair<data_ptr_t, size_t>>& result, uint64_t id) {
        if (root_ == nullptr) {
            return;
        }

        auto node = find_leaf_node_(id);
        if (node) {
            node->get_items(result, id);
            node->unlock_shared();
        }
    }
    size_t btree_t::size() const { return item_count_; }

    btree_t::leaf_node_t* btree_t::find_leaf_node_(uint64_t id) {
        tree_mutex_.lock_shared();

        if (root_ == nullptr) {
            tree_mutex_.unlock_shared();
            return nullptr;
        }

        base_node_t* current_node = root_;
        base_node_t* parent = nullptr;

        // Get the shared latch of next node, release the root_latch
        current_node->lock_shared();
        tree_mutex_.unlock_shared();

        // Traversing Down to the right leaf node
        while (current_node->is_inner_node()) {
            if (parent) {
                parent->unlock_shared();
            }
            parent = current_node;
            current_node = static_cast<inner_node_t*>(current_node)->find_node(id);
            current_node->lock_shared();
        }

        if (parent) {
            parent->unlock_shared();
        }
        return static_cast<leaf_node_t*>(current_node);
    }

    void btree_t::release_locks_(std::deque<base_node_t*>& modified_nodes) const {
        while (!modified_nodes.empty()) {
            modified_nodes.back()->unlock_exclusive();
            modified_nodes.pop_back();
        }
    }

    uint64_t btree_t::get_unique_id_() {
        if (missed_ids_.empty()) {
            return leaf_nodes_count_;
        } else {
            uint64_t result = missed_ids_.front();
            missed_ids_.pop();
            return result;
        }
    }

} // namespace core::b_plus_tree