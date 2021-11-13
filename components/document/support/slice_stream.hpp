#pragma once

#include "slice.hpp"
#include <optional>

namespace document {

class slice_ostream
{
public:
    slice_ostream(void* begin, size_t cap);
    explicit slice_ostream(slice_t s);
    slice_ostream capture() const;

    template <class CALLBACK>
    static alloc_slice_t alloced(size_t max_size, const CALLBACK &writer) {
        alloc_slice_t buf(max_size);
        slice_ostream out(buf);
        if (!writer(out) || out.overflowed())
            return null_slice;
        buf.shorten(out.bytes_written_size());
        return buf;
    }

    slice_t output() const noexcept PURE;
    size_t bytes_written_size() const noexcept PURE;
    size_t capacity() const noexcept PURE;
    bool full() const noexcept PURE;
    bool overflowed() const noexcept PURE;

    bool write(const void *src, size_t size) noexcept;
    bool write(slice_t s) noexcept;
    bool write_byte(uint8_t) noexcept;
    bool write_hex(pure_slice_t src) noexcept;
    bool write_hex(uint64_t) noexcept;
    bool write_decimal(uint64_t) noexcept;
    bool write_uvar_int(uint64_t) noexcept;

    MUST_USE_RESULT void* next() noexcept;
    mutable_slice_t buffer() noexcept;
    bool advance_to(void *next) noexcept;
    bool advance(size_t n) noexcept;
    void retreat(size_t n);

private:
    slice_ostream(const slice_ostream&) = default;

    void* _begin;
    uint8_t* _next;
    uint8_t* _end;
    bool _overflowed {false};
};


inline slice_ostream& operator<< (slice_ostream &out, slice_t data) noexcept {
    out.write(data);
    return out;
}
inline slice_ostream& operator<< (slice_ostream &out, uint8_t byte) noexcept {
    out.write_byte(byte);
    return out;
}


struct slice_istream : public slice_t
{
    constexpr slice_istream(const slice_t &s) noexcept                                               : slice_t(s) {}
    constexpr slice_istream(const alloc_slice_t &s) noexcept                                         : slice_t(s){}
    constexpr slice_istream(const void* b, size_t s) noexcept STEPOVER STEPOVER                 : slice_t(b, s) {}
    constexpr slice_istream(const void* s NONNULL, const void* e NONNULL) noexcept STEPOVER STEPOVER : slice_t(s, e) {}
    size_t bytes_remaining() const noexcept PURE;
    bool eof() const noexcept PURE;

    slice_t read_all(size_t n_bytes) noexcept;
    slice_t read_at_most(size_t n_bytes) noexcept;
    bool read_all(void *dst_buf, size_t dst_size) noexcept;
    size_t read_at_most(void *dst_buf, size_t dst_size) noexcept;
    slice_t read_to_delimiter(slice_t delim) noexcept;
    slice_t read_to_delimiter_or_end(slice_t delim) noexcept;
    slice_t read_bytes_in_set(slice_t set) noexcept;
    uint8_t read_byte() noexcept;
    uint8_t peek_byte() const noexcept PURE;
    slice_t peek() const noexcept PURE;

    MUST_USE_RESULT const void* next() const noexcept PURE;
    void skip(size_t n);
    void skip_to(const void *pos);
    void rewind_to(const void *pos);

    uint64_t read_hex() noexcept;
    uint64_t read_decimal() noexcept;
    int64_t read_signed_decimal() noexcept;
    std::optional<uint64_t> read_uvar_int() noexcept;
    std::optional<uint32_t> read_uvar_int32() noexcept;

private:
    slice_istream(const slice_istream&) = default;
};

}
