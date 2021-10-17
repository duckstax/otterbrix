#include "mutable_hash_tree.hpp"
#include "hash_tree.hpp"
#include "hash_tree_internal.hpp"
#include "node_ref.hpp"
#include "mutable_node.hpp"
#include "mutable.hpp"
#include "bitmap.hpp"
#include "mutable_array.hpp"
#include "mutable_dict.hpp"
#include <algorithm>
#include <ostream>
#include <string>
#include "better_assert.hpp"

namespace storage {

using namespace hashtree;

mutable_hash_tree_t::mutable_hash_tree_t()
{}

mutable_hash_tree_t::mutable_hash_tree_t(const hash_tree_t *tree)
    : _im_root(tree)
{}

mutable_hash_tree_t::~mutable_hash_tree_t() {
    if (_root)
        _root->delete_tree();
}

mutable_hash_tree_t& mutable_hash_tree_t::operator= (mutable_hash_tree_t &&other) noexcept {
    _im_root = other._im_root;
    if (_root)
        _root->delete_tree();
    _root = other._root;
    other._im_root = nullptr;
    other._root = nullptr;
    return *this;
}

mutable_hash_tree_t& mutable_hash_tree_t::operator= (const hash_tree_t *other) {
    _im_root = other;
    if (_root)
        _root->delete_tree();
    _root = nullptr;
    return *this;
}

unsigned mutable_hash_tree_t::count() const {
    if (_root)
        return _root->leaf_count();
    else if (_im_root)
        return _im_root->count();
    else
        return 0;
}

bool mutable_hash_tree_t::is_changed() const {
    return _root != nullptr;
}

node_ref_t mutable_hash_tree_t::root_node() const {
    if (_root)
        return _root;
    else if (_im_root)
        return _im_root->root_node();
    else
        return {};
}

value_t mutable_hash_tree_t::get(slice_t key) const {
    if (_root) {
        target_t target(key);
        node_ref_t leaf = _root->find_nearest(target.hash);
        if (leaf) {
            if (leaf.is_mutable()) {
                auto mleaf = (mutable_leaf_t*)leaf.as_mutable();
                if (mleaf->matches(target))
                    return mleaf->_value;
            } else {
                if (leaf.as_immutable()->leaf.matches(key))
                    return leaf.as_immutable()->leaf.value();
            }
        }
    } else if (_im_root) {
        return _im_root->get(key);
    }
    return nullptr;
}

bool mutable_hash_tree_t::insert(slice_t key, insert_callback callback) {
    if (!_root)
        _root = mutable_interior_t::new_root(_im_root);
    auto result = _root->insert(target_t(key, &callback), 0);
    if (!result)
        return false;
    _root = result;
    return true;
}

void mutable_hash_tree_t::set(slice_t key, value_t value) {
    if (value)
        insert(key, [=](value_t){ return value; });
    else
        remove(key);
}

bool mutable_hash_tree_t::remove(slice_t key) {
    if (!_root) {
        if (!_im_root)
            return false;
        _root = mutable_interior_t::new_root(_im_root);
    }
    return _root->remove(target_t(key), 0);
}

mutable_array_t mutable_hash_tree_t::get_mutable_array(slice_t key) {
    mutable_array_t result;
    insert(key, [&](value_t value) -> value_t {
        auto array = value.as_array();
        result = array.as_mutable();
        if (!result)
            result = array.mutable_copy();
        return result;
    });
    return result;
}

mutable_dict_t mutable_hash_tree_t::get_mutable_dict(slice_t key) {
    mutable_dict_t result;
    insert(key, [&](value_t value) -> value_t {
        auto dict = value.as_dict();
        result = dict.as_mutable();
        if (!result)
            result = dict.mutable_copy();
        return result;
    });
    return result;
}

uint32_t mutable_hash_tree_t::write_to(encoder_t &enc) {
    if (_root) {
        return _root->write_root_to(enc);
    } else if (_im_root) {
        std::unique_ptr<mutable_interior_t> temp_root(mutable_interior_t::new_root(_im_root));
        return temp_root->write_root_to(enc);
    } else {
        return 0;
    }
}

void mutable_hash_tree_t::dump(std::ostream &out) {
    if (_im_root && !_root) {
        _im_root->dump(out);
    } else {
        out << "mutable_hash_tree_t {";
        if (_root) {
            out << "\n";
            _root->dump(out);
        }
        out << "}\n";
    }
}

namespace hashtree {

struct iterator_impl_t {

    static constexpr size_t max_depth = (8*sizeof(hash_t) + bit_shift - 1) / bit_shift;

    struct pos {
        node_ref_t parent;
        int index;
    };

    node_ref_t node;
    pos current;
    pos stack[max_depth];
    unsigned depth;

    iterator_impl_t(node_ref_t root)
        : current {root, -1}
        , depth {0}
    {}

    std::pair<slice_t,value_t> next() {
        while (unsigned(++current.index) >= current.parent.child_count()) {
            if (depth > 0) {
                current = stack[--depth];
            } else {
                node.reset();
                return {};
            }
        }
        while (true) {
            node = current.parent.child_at_index(current.index);
            if (node.is_leaf())
                break;
            assert(depth < max_depth);
            stack[depth++] = current;
            current = {node, 0};
        }
        if (node.is_mutable()) {
            auto leaf = ((mutable_leaf_t*)node.as_mutable());
            return {leaf->_key, leaf->_value};
        } else {
            auto &leaf = node.as_immutable()->leaf;
            return {leaf.key_string(), leaf.value()};
        }
    }
};

}


hash_tree_t::iterator_t::iterator_t(const mutable_hash_tree_t &tree)
    : iterator_t(tree.root_node())
{}

hash_tree_t::iterator_t::iterator_t(const hash_tree_t *tree)
    : iterator_t(tree->root_node())
{}

hash_tree_t::iterator_t::iterator_t(node_ref_t root)
    : _impl(new iterator_impl_t(root))
{
    if (!_impl->current.parent)
        _value = nullptr;
    else
        std::tie(_key, _value) = _impl->next();
}

hash_tree_t::iterator_t::~iterator_t() = default;

hash_tree_t::iterator_t& mutable_hash_tree_t::iterator_t::operator++() {
    std::tie(_key, _value) = _impl->next();
    return *this;
}

}
