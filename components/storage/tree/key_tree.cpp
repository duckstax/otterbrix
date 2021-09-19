#include "key_tree.hpp"
#include "varint.hpp"
#include <math.h>
#include <iostream>
#include <algorithm>
#include "slice_stream.hpp"
#include "platform_compat.hpp"
#include "better_assert.hpp"

namespace storage {

class key_tree_writer_t {
public:
    key_tree_writer_t(const std::vector<slice_t> &strings)
        : _strings(strings)
        , _sizes(strings.size())
    {}

    alloc_slice_t write_tree() {
        auto n = _strings.size();
        size_t total_size = 1 + size_key_tree(0, n);
        alloc_slice_t output(total_size);
        _out = (uint8_t*)output.buf;

        write_byte((uint8_t)ceil(log2(n)));
        write_key_tree_t(0, n);
        assert_postcondition(_out == output.end());
        return output;
    }

private:
    const std::vector<slice_t> &_strings;
    std::vector<size_t> _sizes;
    uint8_t *_out;

    size_t size_key_tree(size_t begin, size_t end) {
        size_t mid = (begin + end) / 2;
        slice_t str = _strings[mid];
        size_t size = size_of_var_int(str.size) + str.size;
        if (end - begin > 1) {
            size_t left_size = size_key_tree(begin, mid);
            if (mid+1 < end) {
                size += size_of_var_int(left_size);
                size += left_size;
                size += size_key_tree(mid+1, end);
            } else {
                size += 1;
                size += left_size;
            }
        }
        _sizes[mid] = size;
        return size;
    }

    void write_key_tree_t(size_t begin, size_t end)
    {
        size_t mid = (begin + end) / 2;
        slice_t str = _strings[mid];
        write_var_int(str.size);
        write(str);

        if (end - begin > 1) {
            if (mid+1 < end) {
                size_t left_size = _sizes[(begin + mid) / 2];
                write_var_int(left_size);
                write_key_tree_t(begin, mid);
                write_key_tree_t(mid+1, end);
            } else {
                write_byte(0);
                write_key_tree_t(begin, mid);
            }
        }
    }

    inline void write_byte(uint8_t byte) {
        *_out++ = byte;
    }

    inline void write(slice_t s) {
        s.copy_to(_out);
        _out += s.size;
    }

    inline size_t write_var_int(size_t n) {
        size_t size = put_uvar_int(_out, n);
        _out += size;
        return size;
    }
};


static int32_t read_var_int(const uint8_t* &tree) {
    slice_istream buf(tree, max_varint_len32);
    std::optional<uint32_t> n = buf.read_uvar_int();
    if (!n)
        return -1;
    tree = (const uint8_t*)buf.buf;
    return *n;
}

static slice_t read_key(const uint8_t* &tree) {
    slice_t key;
    int32_t len = read_var_int(tree);
    if (len >= 0) {
        key = slice_t(tree, len);
        tree += len;
    }
    return key;
}


key_tree_t key_tree_t::from_sorted_strings(const std::vector<slice_t>& strings) {
    return key_tree_t(key_tree_writer_t(strings).write_tree());
}

key_tree_t key_tree_t::from_strings(std::vector<slice_t> strings) {
    std::sort(strings.begin(), strings.end());
    return from_sorted_strings(strings);
}

key_tree_t::key_tree_t(const void *encoded_data_start)
    : _data(encoded_data_start)
{}

key_tree_t::key_tree_t(alloc_slice_t encoded_data)
    : _owned_data(encoded_data)
    , _data(encoded_data.buf)
{}

unsigned key_tree_t::operator[] (slice_t str) const {
    const uint8_t* tree = (const uint8_t*)_data;
    unsigned id = 0;
    unsigned mask = 1;
    for (unsigned depth = *tree++; depth > 0; --depth) {
        slice_t key = read_key(tree);
        if (!key.buf)
            return 0;
        int cmp = str.compare(key);
        if (cmp == 0)
            return id | mask;
        if (depth == 1)
            return 0;
        int32_t left_tree_size = read_var_int(tree);
        if (left_tree_size < 0)
            return 0;
        if (cmp > 0) {
            tree += left_tree_size;
            id |= mask;
        }
        mask <<= 1;
    }
    return 0;
}

slice_t key_tree_t::operator[] (unsigned id) const {
    if (id == 0)
        return null_slice;
    const uint8_t* tree = (const uint8_t*)_data;
    for (unsigned depth = *tree++; depth > 0; --depth) {
        slice_t key = read_key(tree);
        if (!key.buf)
            return key;
        if (id == 1)
            return key;
        if (depth == 1)
            break;
        int32_t left_tree_size = read_var_int(tree);
        if (left_tree_size < 0)
            return null_slice;
        if (id & 1) {
            if (left_tree_size == 0)
                return null_slice;
            tree += left_tree_size;
        }
        id >>= 1;
    }
    return null_slice;
}

slice_t key_tree_t::encoded_data() const  {
    return _owned_data;
}

}
