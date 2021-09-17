#pragma once

#include "slice.hpp"
#include "small_vector.hpp"
#include <stdio.h>
#include <vector>
#include "better_assert.hpp"

namespace storage {

class writer_t {
public:
    static constexpr size_t default_initial_capacity = 256;

    explicit writer_t(size_t initial_capacity = default_initial_capacity);
    explicit writer_t(FILE *output_file NONNULL);
    ~writer_t();

    writer_t(writer_t &&w) noexcept;
    writer_t& operator= (writer_t&& w) noexcept;

    size_t length() const;

    const void* write(slice_t s);
    const void* write(const void* data NONNULL, size_t length);

    writer_t& operator<< (uint8_t byte);
    writer_t& operator<< (slice_t s);

    void pad_to_even_length();

    static slice_t encode_base64(slice_t data);
    static slice_t decode_base64(slice_t data);

    void write_base64(slice_t data);
    void write_decoded_base64(slice_t base64str);

    void* reserve_space(size_t length);

    template <class T>
    T* reserve_space(size_t count)  {
        return (T*) reserve_space(count * sizeof(T));
    }

    template <class T>
    void* write(size_t max_length, T callback) {
        auto dst = (uint8_t*)reserve_space(max_length);
        size_t used_length = callback(dst);
        _available.move_start((size_t)used_length - (size_t)max_length);
        return dst;
    }

    std::vector<slice_t> output() const;
    alloc_slice_t copy_output() const;
    void copy_output_to(void *dst) const;

    template <class T>
    void foreach_chunk(T callback) const {
        assert_precondition(!_output_file);
        auto n = _chunks.size();
        for (auto chunk : _chunks) {
            if (_usually_false(--n == 0)) {
                chunk.set_size(chunk.size - _available.size);
                if (chunk.size == 0)
                    continue;
            }
            callback(chunk);
        }
    }

    bool write_output_to_file(FILE *f);

    void reset();
    alloc_slice_t finish();

    template <class T>
    void finish(T callback) {
        if (_chunks.size() == 1) {
            slice_t chunk = _chunks[0];
            chunk.set_size(chunk.size - _available.size);
            callback(chunk);
            reset();
        } else {
            alloc_slice_t output = finish();
            callback(output);
        }
    }

    FILE* output_file() const;
    void flush();

private:
    void _reset();
    void* _write(const void* data_or_null, size_t length);
    void* write_to_new_chunk(const void* data_or_null, size_t length);
    void add_chunk(size_t capacity);
    void free_chunk(slice_t);
    void migrate_initial_buf(const writer_t& other);

    writer_t(const writer_t&) = delete;
    const writer_t& operator=(const writer_t&) = delete;

    void assert_length_correct() const;

    slice_t _available;
    small_vector_t<slice_t, 4> _chunks;
    size_t _chunk_size;
    size_t _length {0};
    FILE* _output_file;
    uint8_t _initial_buf[default_initial_capacity];
};

}
