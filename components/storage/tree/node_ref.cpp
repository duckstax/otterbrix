#include "node_ref.hpp"
#include "mutable_node.hpp"
#include "better_assert.hpp"

namespace storage { namespace hashtree {

target_t::target_t(slice_t k, mutable_hash_tree_t::insert_callback *callback)
    : key(k), hash(compute_hash(k)), insert_callback(callback)
{}

bool target_t::operator==(const target_t &b) const {
    return hash == b.hash && key == b.key;
}


node_ref_t::operator bool() const {
    return _addr != 0;
}

node_ref_t::node_ref_t()
    : _addr(0)
{}

node_ref_t::node_ref_t(mutable_node_t *n)
    : _addr(size_t(n) | 1)
{
    assert_precondition(n);
}

node_ref_t::node_ref_t(const node_t *n)
    : _addr(size_t(n))
{}

node_ref_t::node_ref_t(const leaf_t *n)
    : _addr(size_t(n))
{}

node_ref_t::node_ref_t(const interior_t *n)
    : _addr(size_t(n))
{}

void node_ref_t::reset() {
    _addr = 0;
}

bool node_ref_t::is_mutable() const {
    return (_addr & 1) != 0;
}

mutable_node_t *node_ref_t::as_mutable() const  {
    return is_mutable() ? _as_mutable() : nullptr;
}

const node_t *node_ref_t::as_immutable() const  {
    return is_mutable() ? nullptr : _as_immutable();
}

bool node_ref_t::is_leaf() const {
    return is_mutable() ? _as_mutable()->is_leaf() : _as_immutable()->is_leaf();
}

hash_t node_ref_t::hash() const {
    assert_precondition(is_leaf());
    return is_mutable() ? ((mutable_leaf_t*)_as_mutable())->_hash : _as_immutable()->leaf.hash();
}

value_t node_ref_t::value() const {
    assert_precondition(is_leaf());
    return is_mutable() ? ((mutable_leaf_t*)_as_mutable())->_value : _as_immutable()->leaf.value();
}

bool node_ref_t::matches(target_t target) const {
    assert_precondition(is_leaf());
    return is_mutable() ? ((mutable_leaf_t*)_as_mutable())->matches(target) : _as_immutable()->leaf.matches(target.key);
}

unsigned node_ref_t::child_count() const {
    assert_precondition(!is_leaf());
    return is_mutable() ? ((mutable_interior_t*)_as_mutable())->child_count() : _as_immutable()->interior.child_count();
}

node_ref_t node_ref_t::child_at_index(unsigned index) const {
    assert_precondition(!is_leaf());
    return is_mutable() ? ((mutable_interior_t*)_as_mutable())->child_at_index(index) : _as_immutable()->interior.child_at_index(index);
}

node_t node_ref_t::write_to(encoder_t &enc) {
    assert_precondition(!is_leaf());
    node_t node;
    if (is_mutable())
        node.interior = ((mutable_interior_t*)as_mutable())->write_to(enc);
    else
        node.interior = as_immutable()->interior.write_to(enc);
    return node;
}

uint32_t node_ref_t::write_to(encoder_t &enc, bool write_key) {
    assert_precondition(is_leaf());
    if (is_mutable())
        return ((mutable_leaf_t*)as_mutable())->write_to(enc, write_key);
    else
        return as_immutable()->leaf.write_to(enc, write_key);
}

void node_ref_t::dump(std::ostream &out, unsigned indent) const {
    if (is_mutable())
        is_leaf() ? ((mutable_leaf_t*)_as_mutable())->dump(out, indent)
                  : ((mutable_interior_t*)_as_mutable())->dump(out, indent);
    else
        is_leaf() ? _as_immutable()->leaf.dump(out, indent)
                  : _as_immutable()->interior.dump(out, indent);
}

mutable_node_t *node_ref_t::_as_mutable() const {
    return (mutable_node_t*)(_addr & ~1);
}

const node_t *node_ref_t::_as_immutable() const {
    return (const node_t*)_addr;
}

} } 
