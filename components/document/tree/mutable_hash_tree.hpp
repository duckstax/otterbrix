#pragma once

#include "hash_tree.hpp"
#include "slice.hpp"
#include <functional>
#include <memory>

namespace storage {

class mutable_array_t;
class mutable_dict_t;
class encoder_t;

namespace hashtree {
class mutable_interior_t;
class node_ref_t;
}


class mutable_hash_tree_t {
public:
    using insert_callback = std::function<value_t(value_t)>;
    using iterator_t = hash_tree_t::iterator_t;

    mutable_hash_tree_t();
    mutable_hash_tree_t(const hash_tree_t *tree);
    ~mutable_hash_tree_t();

    mutable_hash_tree_t& operator= (mutable_hash_tree_t &&other) noexcept;
    mutable_hash_tree_t& operator= (const hash_tree_t *other);

    value_t get(slice_t key) const;

    mutable_array_t get_mutable_array(slice_t key);
    mutable_dict_t get_mutable_dict(slice_t key);

    unsigned count() const;
    bool is_changed() const;

    void set(slice_t key, value_t value);
    bool insert(slice_t key, insert_callback callback);
    bool remove(slice_t key);

    uint32_t write_to(encoder_t &enc);

    void dump(std::ostream &out);

private:
    hashtree::node_ref_t root_node() const;

    const hash_tree_t* _im_root {nullptr};
    hashtree::mutable_interior_t* _root {nullptr};

    friend class hash_tree_t::iterator_t;
};

} 
