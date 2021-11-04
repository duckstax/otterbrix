#pragma once

#include "mutable_hash_tree.hpp"
#include "hash_tree.hpp"
#include "hash_tree_internal.hpp"
#include <ostream>
#include "better_assert.hpp"

namespace document { namespace hashtree {

class mutable_node_t;


struct target_t {
    explicit target_t(slice_t k, mutable_hash_tree_t::insert_callback *callback = nullptr);

    bool operator== (const target_t &b) const;

    slice_t const key;
    hash_t const hash;
    mutable_hash_tree_t::insert_callback *insert_callback {nullptr};
};


class node_ref_t {
public:
    node_ref_t();
    node_ref_t(mutable_node_t* n);
    node_ref_t(const node_t* n);
    node_ref_t(const leaf_t* n);
    node_ref_t(const interior_t* n);

    void reset();
    operator bool () const PURE;
    bool is_mutable() const PURE;
    mutable_node_t* as_mutable() const PURE;
    const node_t* as_immutable() const PURE;
    bool is_leaf() const PURE;
    hash_t hash() const PURE;
    bool matches(target_t target) const PURE;
    value_t value() const PURE;
    unsigned child_count() const PURE;
    node_ref_t child_at_index(unsigned index) const PURE;
    node_t write_to(encoder_t &enc);
    uint32_t write_to(encoder_t &enc, bool write_key);
    void dump(std::ostream &out, unsigned indent) const;

private:
    mutable_node_t* _as_mutable() const;
    const node_t* _as_immutable() const;

    size_t _addr;
};

} } 
