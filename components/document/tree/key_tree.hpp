#pragma once

#include "slice.hpp"
#include <vector>

namespace document {

class key_tree_t {
public:
    key_tree_t(const void *encoded_data_start);
    key_tree_t(alloc_slice_t encoded_data);

    static key_tree_t from_sorted_strings(const std::vector<slice_t> &strings);
    static key_tree_t from_strings(std::vector<slice_t> strings);

    unsigned operator[] (slice_t str) const;
    slice_t operator[] (unsigned id) const;

    slice_t encoded_data() const;

private:
    alloc_slice_t _owned_data;
    const void * _data;
};

}
