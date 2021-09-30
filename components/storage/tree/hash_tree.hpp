#pragma once

#include "slice.hpp"
#include "storage.hpp"
#include <memory>

namespace storage {

class mutable_hash_tree_t;

namespace hashtree {
class interior_t;
class mutable_interior_t;
class node_ref_t;
struct iterator_impl_t;
}


class hash_tree_t {
public:
    class iterator_t {
    public:
        iterator_t(const mutable_hash_tree_t&);
        iterator_t(const hash_tree_t*);
        iterator_t(iterator_t&&);
        ~iterator_t();
        slice_t key() const noexcept               { return _key; }
        value_t value() const noexcept             { return _value; }
        explicit operator bool() const noexcept    { return !!_value; }
        iterator_t& operator ++();

    private:
        iterator_t(hashtree::node_ref_t);
        std::unique_ptr<hashtree::iterator_impl_t> _impl;
        slice_t _key;
        value_t _value;
    };

    static const hash_tree_t* from_data(slice_t data);

    value_t get(slice_t) const;
    unsigned count() const;
    void dump(std::ostream &out) const;

private:
    const hashtree::interior_t* root_node() const;

    friend class hashtree::mutable_interior_t;
    friend class mutable_hash_tree_t;
};

}
