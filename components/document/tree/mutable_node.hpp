#pragma once

#include "node_ref.hpp"
#include "platform_compat.hpp"
#include "ref_counted.hpp"
#include "mutable.hpp"
#include "slice.hpp"
#include "temp_array.hpp"
#include "better_assert.hpp"

namespace storage { namespace hashtree {

using offset_t = int32_t;


class mutable_node_t {
public:
    mutable_node_t(unsigned capacity)
        : _capacity(int8_t(capacity))
    {
        assert_precondition(capacity <= max_children);
    }

    bool is_leaf() const PURE {
        return _capacity == 0;
    }

    static void encode_offset(offset_t &o, size_t cur_pos) {
        assert_precondition((ssize_t)cur_pos > o);
        o = endian::little_enc32(offset_t(cur_pos - o));
    }

protected:
    uint8_t capacity() const PURE {
        assert_precondition(_capacity > 0);
        return _capacity;
    }

    int8_t _capacity;
};


class mutable_leaf_t : public mutable_node_t {
public:
    mutable_leaf_t(const target_t &t, value_t v)
        : mutable_node_t(0)
        , _key(t.key)
        , _hash(t.hash)
        , _value(v)
    {}

    bool matches(target_t target) const {
        return _hash == target.hash && _key == target.key;
    }

    uint32_t write_to(encoder_t &enc, bool write_key) {
        if (write_key)
            enc.write_string(_key);
        else
            enc.write_value(_value);
        return (uint32_t)enc.finish_item();
    }

    void dump(std::ostream &out, unsigned indent) {
        char hash_str[30];
        sprintf(hash_str, "{%08x ", _hash);
        out << std::string(2*indent, ' ') << hash_str << '"';
        out.write((char*)_key.buf, _key.size);
        out << "\"=" << _value.to_json_string() << "}";
    }

    alloc_slice_t const _key;
    hash_t const _hash;
    retained_value_t _value;
};


class mutable_interior_t : public mutable_node_t {
public:
    static mutable_interior_t* new_root(const hash_tree_t *tree) {
        if (tree)
            return mutable_copy(tree->root_node());
        else
            return new_node(max_children);
    }

    unsigned child_count() const {
        return _bitmap.bit_count();
    }

    node_ref_t child_at_index(unsigned index) {
        assert_precondition(index < capacity());
        return _children[index];
    }

    void delete_tree() {
        unsigned n = child_count();
        for (unsigned i = 0; i < n; ++i) {
            auto child = _children[i].as_mutable();
            if (child) {
                if (child->is_leaf())
                    delete (mutable_leaf_t*)child;
                else
                    ((mutable_interior_t*)child)->delete_tree();
            }
        }
        delete this;
    }

    unsigned leaf_count() const {
        unsigned count = 0;
        unsigned n = child_count();
        for (unsigned i = 0; i < n; ++i) {
            auto child = _children[i];
            if (child.is_mutable()) {
                if (child.as_mutable()->is_leaf())
                    count += 1;
                else
                    count += ((mutable_interior_t*)child.as_mutable())->leaf_count();
            } else {
                if (child.as_immutable()->is_leaf())
                    count += 1;
                else
                    count += child.as_immutable()->interior.leaf_count();
            }
        }
        return count;
    }

    node_ref_t find_nearest(hash_t hash) const {
        unsigned bit_no = child_bit_number(hash);
        if (!has_child(bit_no))
            return node_ref_t();
        node_ref_t child = child_for_bit_number(bit_no);
        if (child.is_leaf()) {
            return child;
        } else if (child.is_mutable()) {
            auto mchild = child.as_mutable();
            return ((mutable_interior_t*)mchild)->find_nearest(hash >> bit_shift);
        } else {
            auto ichild = child.as_immutable();
            return ichild->interior.find_nearest(hash >> bit_shift);
        }
    }

    mutable_interior_t* insert(const target_t &target, unsigned shift) {
        assert_precondition(shift + bit_shift < 8*sizeof(hash_t));
        unsigned bit_no = child_bit_number(target.hash, shift);
        if (!has_child(bit_no)) {
            value_t val = (*target.insert_callback)(nullptr);
            if (!val)
                return nullptr;
            return add_child(bit_no, new mutable_leaf_t(target, val));
        }
        node_ref_t &child_ref = child_for_bit_number(bit_no);
        if (child_ref.is_leaf()) {
            if (child_ref.matches(target)) {
                value_t val = (*target.insert_callback)(child_ref.value());
                if (!val)
                    return nullptr;
                if (child_ref.is_mutable())
                    ((mutable_leaf_t*)child_ref.as_mutable())->_value = val;
                else
                    child_ref = new mutable_leaf_t(target, val);
                return this;
            } else {
                mutable_interior_t *node = promote_leaf(child_ref, shift);
                auto inserted_node = node->insert(target, shift+bit_shift);
                if (!inserted_node) {
                    delete node;
                    return nullptr;
                }
                child_ref = inserted_node;
                return this;
            }
        } else {
            auto child = (mutable_interior_t*)child_ref.as_mutable();
            if (!child)
                child = mutable_copy(&child_ref.as_immutable()->interior, 1);
            child = child->insert(target, shift+bit_shift);
            if (child)
                child_ref = child;
            return this;
        }
    }

    bool remove(target_t target, unsigned shift) {
        assert_precondition(shift + bit_shift < 8*sizeof(hash_t));
        unsigned bit_no = child_bit_number(target.hash, shift);
        if (!has_child(bit_no))
            return false;
        unsigned child_index = child_index_for_bit_number(bit_no);
        node_ref_t child_ref = _children[child_index];
        if (child_ref.is_leaf()) {
            if (child_ref.matches(target)) {
                remove_child(bit_no, child_index);
                delete (mutable_leaf_t*)child_ref.as_mutable();
                return true;
            } else {
                return false;
            }
        } else {
            auto child = (mutable_interior_t*)child_ref.as_mutable();
            if (child) {
                if (!child->remove(target, shift+bit_shift))
                    return false;
            } else {
                child = mutable_copy(&child_ref.as_immutable()->interior);
                if (!child->remove(target, shift+bit_shift)) {
                    delete child;
                    return false;
                }
                _children[child_index] = child;
            }
            if (child->_bitmap.empty()) {
                remove_child(bit_no, child_index);
                delete child;
            }
            return true;
        }
    }

    static offset_t encode_immutable_offset(const node_t *inode, offset_t off, const encoder_t &enc) {
        ssize_t o = (((char*)inode - (char*)enc.base().buf) - off) - enc.base().size;
        assert(o < 0 && o > INT32_MIN);
        return offset_t(o);
    }

    interior_t write_to(encoder_t &enc) {
        unsigned n = child_count();
        _temp_array(nodes, node_t, n);
        for (unsigned i = 0; i < n; ++i) {
            if (!_children[i].is_leaf())
                nodes[i] = _children[i].write_to(enc);
        }
        for (unsigned i = 0; i < n; ++i) {
            if (_children[i].is_leaf())
                nodes[i].leaf._value_offset = _children[i].write_to(enc, false);
        }
        for (unsigned i = 0; i < n; ++i) {
            if (_children[i].is_leaf())
                nodes[i].leaf._key_offset = _children[i].write_to(enc, true);
        }
        const offset_t children_pos = (offset_t)enc.next_write_pos();
        auto cur_pos = children_pos;
        for (unsigned i = 0; i < n; ++i) {
            auto &node = nodes[i];
            if (_children[i].is_leaf())
                node.leaf.make_relative_to(cur_pos);
            else
                node.interior.make_relative_to(cur_pos);
            cur_pos += sizeof(nodes[i]);
        }
        enc.write_raw({nodes, n * sizeof(nodes[0])});
        return interior_t(bitmap_bit_t(_bitmap), children_pos);
    }

    offset_t write_root_to(encoder_t &enc) {
        auto int_node = write_to(enc);
        auto cur_pos = (offset_t)enc.next_write_pos();
        int_node.make_relative_to(cur_pos);
        enc.write_raw({&int_node, sizeof(int_node)});
        return offset_t(cur_pos);
    }

    void dump(std::ostream &out, unsigned indent = 1) const {
        unsigned n = child_count();
        out << std::string(2*indent, ' ') << "{";
        for (unsigned i = 0; i < n; ++i) {
            out << "\n";
            _children[i].dump(out, indent+1);
        }
        out << " }";
    }

    static void operator delete(void* ptr) {
        ::operator delete(ptr);
    }

private:
    mutable_interior_t() = delete;
    mutable_interior_t(const mutable_interior_t& i) = delete;
    mutable_interior_t(mutable_interior_t&& i) = delete;
    mutable_interior_t& operator=(const mutable_interior_t&) = delete;

    static mutable_interior_t* new_node(unsigned capacity, mutable_interior_t *orig = nullptr) {
        return new (capacity) mutable_interior_t(capacity, orig);
    }

    static void* operator new(size_t size, unsigned capacity) {
        return ::operator new(size + capacity*sizeof(node_ref_t));
    }

    static void operator delete(void* ptr, unsigned) {
        ::operator delete(ptr);
    }

    static mutable_interior_t* mutable_copy(const interior_t *it_node, unsigned extra_capacity = 0) {
        auto child_count = it_node->child_count();
        auto node = new_node(child_count + extra_capacity);
        node->_bitmap = as_bitmap(it_node->bitmap());
        for (unsigned i = 0; i < child_count; ++i)
            node->_children[i] = node_ref_t(it_node->child_at_index(i));
        return node;
    }

    static mutable_interior_t* promote_leaf(node_ref_t& child_leaf, unsigned shift) {
        unsigned level = shift / bit_shift;
        mutable_interior_t* node = new_node(2 + (level<1) + (level<3));
        unsigned child_bit_no = child_bit_number(child_leaf.hash(), shift+bit_shift);
        node = node->add_child(child_bit_no, child_leaf);
        return node;
    }

    mutable_interior_t(unsigned cap, mutable_interior_t* orig = nullptr)
        : mutable_node_t(cap)
        , _bitmap(orig ? orig->_bitmap : bitmap_t<bitmap_bit_t>{})
    {
        if (orig)
            memcpy(_children, orig->_children, orig->capacity()*sizeof(node_ref_t));
        else
            memset(_children, 0, cap*sizeof(node_ref_t));
    }

    mutable_interior_t* grow() {
        assert_precondition(capacity() < max_children);
        auto replacement = (mutable_interior_t*)realloc(this, sizeof(mutable_interior_t) + (capacity()+1)*sizeof(node_ref_t));
        if (!replacement)
            throw std::bad_alloc();
        replacement->_capacity++;
        return replacement;
    }

    static unsigned child_bit_number(hash_t hash, unsigned shift = 0)  {
        return (hash >> shift) & (max_children - 1);
    }

    unsigned child_index_for_bit_number(unsigned bit_no) const {
        return _bitmap.index_of_bit(bit_no);
    }

    bool has_child(unsigned bit_no) const  {
        return _bitmap.contains_bit(bit_no);
    }

    node_ref_t& child_for_bit_number(unsigned bit_no) {
        auto i = child_index_for_bit_number(bit_no);
        assert(i < capacity());
        return _children[i];
    }

    node_ref_t const& child_for_bit_number(unsigned bit_no) const {
        return const_cast<mutable_interior_t*>(this)->child_for_bit_number(bit_no);
    }

    mutable_interior_t* add_child(unsigned bit_no, node_ref_t child) {
        return add_child(bit_no, child_index_for_bit_number(bit_no), child);
    }

    mutable_interior_t* add_child(unsigned bit_no, unsigned child_index, node_ref_t child) {
        mutable_interior_t* node = (child_count() < capacity()) ? this : grow();
        return node->_add_child(bit_no, child_index, child);
    }

    mutable_interior_t* _add_child(unsigned bit_no, unsigned child_index, node_ref_t child) {
        assert_precondition(child);
        memmove(&_children[child_index+1], &_children[child_index], (capacity() - child_index - 1)*sizeof(node_ref_t));
        _children[child_index] = child;
        _bitmap.add_bit(bit_no);
        return this;
    }

    void remove_child(unsigned bit_no, unsigned child_index) {
        assert_precondition(child_index < capacity());
        memmove(&_children[child_index], &_children[child_index+1], (capacity() - child_index - 1)*sizeof(node_ref_t));
        _bitmap.remove_bit(bit_no);
    }


    bitmap_t<bitmap_bit_t> _bitmap {0};
    node_ref_t _children[0];
};

} }
