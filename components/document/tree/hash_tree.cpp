#include "hash_tree.hpp"
#include "hash_tree_internal.hpp"
#include "bitmap.hpp"
#include "endian.hpp"
#include "platform_compat.hpp"
#include "temp_array.hpp"
#include <algorithm>
#include <ostream>
#include <string>
#include "better_assert.hpp"

namespace storage {

#define DEREF(OFF, TYPE)    ((const TYPE*)((uint8_t*)this - (OFF)))
#define DEREF_VALUE(OFF)    value_t((impl_value_t)DEREF(OFF, void))

namespace hashtree {

PURE hash_t compute_hash(slice_t s) noexcept {
    auto byte = (const uint8_t*)s.buf;
    uint32_t h = 2166136261;
    for (size_t i = 0; i < s.size; i++, byte++) {
        h = (h ^ *byte) * 16777619;
    }
    return h;
}

void leaf_t::validate() const {
    assert(_key_offset > 0);
    assert(_value_offset > 0);
}

value_t leaf_t::key() const {
    return DEREF_VALUE(_key_offset);
}

value_t leaf_t::value() const {
    return DEREF_VALUE(_value_offset & ~1);
}

slice_t leaf_t::key_string() const {
    return DEREF_VALUE(_key_offset).as_string();
}

uint32_t leaf_t::write_to(encoder_t &enc, bool write_key) const {
    if (enc.base().is_contains_address(this)) {
        auto pos = int32_t((char*)this - (char*)enc.base().end());
        return pos - (write_key ? _key_offset : _value_offset);
    } else {
        if (write_key)
            enc.write_value(key());
        else
            enc.write_value(value());
        return (uint32_t)enc.finish_item();
    }
}

void leaf_t::dump(std::ostream &out, unsigned indent) const {
    char hash_str[30];
    sprintf(hash_str, "[%08x ", hash());
    out << std::string(2*indent, ' ') << hash_str << '"';
    auto k = key_string();
    out.write((char*)k.buf, k.size);
    out << "\"=" << value().to_json_string() << "]";
}

void interior_t::validate() const {
    assert(_children_offset > 0);
}

bitmap_bit_t interior_t::bitmap() const {
    return endian::little_dec32(_bitmap);
}

bool interior_t::has_child(unsigned bit_no) const {
    return as_bitmap(bitmap()).contains_bit(bit_no);
}

unsigned interior_t::child_count() const {
    return as_bitmap(bitmap()).bit_count();
}

const node_t* interior_t::child_at_index(int i) const {
    assert_precondition(_children_offset > 0);
    return (DEREF(_children_offset, node_t) + i)->validate();
}

const node_t* interior_t::child_for_bit_number(unsigned bit_no) const {
    return has_child(bit_no) ? child_at_index(as_bitmap(bitmap()).index_of_bit(bit_no)) : nullptr;
}

const leaf_t* interior_t::find_nearest(hash_t hash) const {
    const node_t *child = child_for_bit_number( hash & (max_children - 1) );
    if (!child)
        return nullptr;
    else if (child->is_leaf())
        return (const leaf_t*)child;
    else
        return ((const interior_t*)child)->find_nearest(hash >> bit_shift);
}

unsigned interior_t::leaf_count() const {
    unsigned count = 0;
    auto c = child_at_index(0);
    for (unsigned n = child_count(); n > 0; --n, ++c) {
        if (c->is_leaf())
            count += 1;
        else
            count += ((interior_t*)c)->leaf_count();
    }
    return count;
}

void interior_t::dump(std::ostream &out, unsigned indent =1) const {
    unsigned n = child_count();
    out << std::string(2*indent, ' ') << "[";
    auto child = child_at_index(0);
    for (unsigned i = 0; i < n; ++i, ++child) {
        out << "\n";
        if (child->is_leaf())
            child->leaf.dump(out, indent+1);
        else
            child->interior.dump(out, indent+1);
    }
    out << " ]";
}

interior_t interior_t::write_to(encoder_t &enc) const {
    if (enc.base().is_contains_address(this)) {
        auto pos = int32_t((char*)this - (char*)enc.base().end());
        return make_absolute(pos);
    } else {
        unsigned n = child_count();
        _temp_array(nodes, node_t, n);
        for (unsigned i = 0; i < n; ++i) {
            auto child = child_at_index(i);
            if (!child->is_leaf())
                nodes[i].interior = child->interior.write_to(enc);
        }
        for (unsigned i = 0; i < n; ++i) {
            auto child = child_at_index(i);
            if (child->is_leaf())
                nodes[i].leaf._value_offset = child->leaf.write_to(enc, false);
        }
        for (unsigned i = 0; i < n; ++i) {
            auto child = child_at_index(i);
            if (child->is_leaf())
                nodes[i].leaf._key_offset = child->leaf.write_to(enc, true);
        }

        const uint32_t children_pos = (uint32_t)enc.next_write_pos();
        auto curPos = children_pos;
        for (unsigned i = 0; i < n; ++i) {
            auto &node = nodes[i];
            if (child_at_index(i)->is_leaf())
                node.leaf.make_relative_to(curPos);
            else
                node.interior.make_relative_to(curPos);
            curPos += sizeof(nodes[i]);
        }
        enc.write_raw({nodes, n * sizeof(nodes[0])});
        return interior_t(bitmap_bit_t(_bitmap), children_pos);
    }
}

}


using namespace hashtree;

const hash_tree_t* hash_tree_t::from_data(slice_t data) {
    return (const hash_tree_t*)offsetby(data.end(), -(ssize_t)sizeof(interior_t));
}

const interior_t* hash_tree_t::root_node() const {
    return (const interior_t*)this;
}

value_t hash_tree_t::get(slice_t key) const {
    auto root = root_node();
    auto leaf = root->find_nearest(compute_hash(key));
    if (leaf && leaf->key_string() == key)
        return leaf->value();
    return nullptr;
}

unsigned hash_tree_t::count() const {
    return root_node()->leaf_count();
}

void hash_tree_t::dump(std::ostream &out) const {
    out << "hash_tree_t [\n";
    root_node()->dump(out);
    out << "]\n";
}

}
