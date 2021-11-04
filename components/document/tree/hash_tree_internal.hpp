#pragma once

#include "slice.hpp"
#include "storage.hpp"
#include "bitmap.hpp"
#include "endian.hpp"
#include <memory>

namespace document { namespace hashtree {

union node_t;
class mutable_interior_t;

using hash_t = uint32_t;
using bitmap_bit_t = uint32_t;

static constexpr int bit_shift = 5;
static constexpr int max_children = 1 << bit_shift;
static_assert(sizeof(bitmap_bit_t) == max_children / 8, "Wrong constants");

PURE hash_t compute_hash(slice_t key) noexcept;


class leaf_t {
public:
    void validate() const;

    value_t key() const;
    value_t value() const;
    slice_t key_string() const;
    hash_t hash() const               { return compute_hash(key_string()); }
    bool matches(slice_t key) const   { return key_string() == key; }
    void dump(std::ostream&, unsigned indent) const;

    uint32_t key_offset() const       { return _key_offset; }
    uint32_t value_offset() const     { return _key_offset; }

    leaf_t(uint32_t key_pos, uint32_t value_pos)
        : _key_offset(key_pos)
        , _value_offset(value_pos)
    {}

    void make_relative_to(uint32_t pos) {
        _key_offset = pos - _key_offset;
        _value_offset = (pos - _value_offset) | 1;
    }

    leaf_t make_absolute(uint32_t pos) const {
        return leaf_t(pos - _key_offset, pos - (_value_offset & ~1));
    }

    uint32_t write_to(encoder_t&, bool write_key) const;

private:
    endian::uint32_le_unaligned _key_offset;
    endian::uint32_le_unaligned _value_offset;

    friend union node_t;
    friend class interior_t;
    friend class mutable_interior_t;
};


class interior_t {
public:
    void validate() const;

    const leaf_t* find_nearest(hash_t hash) const;
    unsigned leaf_count() const;

    unsigned child_count() const;
    const node_t* child_at_index(int i) const;
    bool has_child(unsigned bit_no) const;
    const node_t* child_for_bit_number(unsigned bit_no) const;

    bitmap_bit_t bitmap() const;

    void dump(std::ostream&, unsigned indent) const;

    uint32_t children_offset() const  { return _children_offset; }

    interior_t(bitmap_bit_t bitmap, uint32_t children_pos)
        : _bitmap(bitmap)
        , _children_offset(children_pos)
    {}

    void make_relative_to(uint32_t pos) {
        _children_offset = pos - _children_offset;
    }

    interior_t make_absolute(uint32_t pos) const {
        return interior_t(_bitmap, pos - _children_offset);
    }

    interior_t write_to(encoder_t&) const;

private:
    endian::uint32_le_unaligned _bitmap;
    endian::uint32_le_unaligned _children_offset;
};


union node_t {
    leaf_t leaf;
    interior_t interior;

    node_t() {}
    bool is_leaf() const  { return (leaf._value_offset & 1) != 0; }

    const node_t* validate() const {
        if (is_leaf()) leaf.validate(); else interior.validate();
        return this;
    }
};

} }

