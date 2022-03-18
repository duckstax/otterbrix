#include "slice_stream.hpp"
#include <components/document/support/varint.hpp>
#include <components/document/support/better_assert.hpp>

namespace document {

static inline uint8_t _hex_digit(int n) {
    static constexpr const uint8_t digits[] = "0123456789abcdef";
    return digits[n];
}

PURE static int _digit_to_int(char ch) noexcept {
    int d = ch - '0';
    if ((unsigned) d < 10)
        return d;
    d = ch - 'a';
    if ((unsigned) d < 6)
        return d + 10;
    d = ch - 'A';
    if ((unsigned) d < 6)
        return d + 10;
    return -1;
}


bool slice_ostream::advance_to(void *pos) noexcept {
    if (_usually_false(pos < _next || pos > _end) ) {
        _overflowed = true;
        return false;
    }
    assert_precondition(pos >= _next && pos <= _end);
    _next = (uint8_t*)pos;
    return true;
}

bool slice_ostream::advance(size_t n) noexcept {
    if (_usually_false(n > capacity())) {
        _overflowed = true;
        return false;
    }
    _next += n;
    return true;
}

void slice_ostream::retreat(size_t n) {
    assert_precondition(n <= bytes_written_size());
    _next -= n;
}

bool slice_ostream::write_byte(uint8_t n) noexcept {
    if (_next >= _end) {
        _overflowed = true;
        return false;
    }
    *_next++ = n;
    return true;
}

slice_ostream::slice_ostream(void *begin, size_t cap)
    : _begin(begin)
    , _next((uint8_t*)begin)
    , _end(_next + cap)
{}

slice_ostream::slice_ostream(slice_t s)
    : slice_ostream((uint8_t*)s.buf, s.size)
{}

slice_ostream slice_ostream::capture() const {
    return slice_ostream(*this);
}

slice_t slice_ostream::output() const noexcept {
    return slice_t(_begin, _next);
}

size_t slice_ostream::bytes_written_size() const noexcept {
    return _next - (uint8_t*)_begin;
}

size_t slice_ostream::capacity() const noexcept {
    return _end - _next;
}

bool slice_ostream::full() const noexcept {
    return _next >= _end;
}

bool slice_ostream::overflowed() const noexcept {
    return _overflowed;
}

bool slice_ostream::write(slice_t s) noexcept {
    return write(s.buf, s.size);
}

MUST_USE_RESULT void* slice_ostream::next() noexcept {
    return _next;
}

mutable_slice_t slice_ostream::buffer() noexcept {
    return {_next, _end};
}

bool slice_ostream::write(const void *src, size_t size) noexcept {
    if (_usually_false(size > capacity())) {
        _overflowed = true;
        return false;
    }
    if (_usually_false(size == 0))
        return true;
    ::memcpy(_next, src, size);
    _next += size;
    return true;
}

bool slice_ostream::write_decimal(uint64_t n) noexcept {
    if (n < 10) {
        return write_byte('0' + (char)n);
    } else {
        char temp[20];
        char *dst = &temp[20];
        size_t len = 0;
        do {
            *(--dst) = '0' + (n % 10);
            n /= 10;
            len++;
        } while (n > 0);
        return write(dst, len);
    }
}

bool slice_ostream::write_hex(uint64_t n) noexcept {
    char temp[16];
    char *dst = &temp[16];
    size_t len = 0;
    do {
        *(--dst) = _hex_digit(n & 0x0F);
        n >>= 4;
        len++;
    } while (n > 0);
    return write(dst, len);
}

bool slice_ostream::write_hex(pure_slice_t src) noexcept {
    if (_usually_false(capacity() < 2 * src.size)) {
        _overflowed = true;
        return false;
    }
    auto dst = _next;
    for (size_t i = 0; i < src.size; ++i) {
        *dst++ = _hex_digit(src[i] >> 4);
        *dst++ = _hex_digit(src[i] & 0x0F);
    }
    _next = dst;
    return true;
}

bool slice_ostream::write_uvar_int(uint64_t n) noexcept {
    if (capacity() < max_varint_len64 && capacity() < size_of_var_int(n)) {
        _overflowed = true;
        return false;
    }
    _next += put_uvar_int(_next, n);
    return true;
}


size_t slice_istream::bytes_remaining() const noexcept {
    return size;
}

bool slice_istream::eof() const noexcept {
    return size == 0;
}

uint8_t slice_istream::peek_byte() const noexcept {
    return (size > 0) ? (*this)[0] : 0;
}

slice_t slice_istream::peek() const noexcept {
    return *this;
}

MUST_USE_RESULT const void* slice_istream::next() const noexcept {
    return buf;
}

void slice_istream::skip(size_t n) {
    slice_t::move_start(n);
}

slice_t slice_istream::read_all(size_t n_bytes) noexcept  {
    if (n_bytes > size)
        return null_slice;
    slice_t result(buf, n_bytes);
    skip(n_bytes);
    return result;
}

slice_t slice_istream::read_at_most(size_t n_bytes) noexcept {
    n_bytes = std::min(n_bytes, size);
    slice_t result(buf, n_bytes);
    skip(n_bytes);
    return result;
}

bool slice_istream::read_all(void *dst_buf, size_t dst_size) noexcept {
    if (dst_size > size)
        return false;
    ::memcpy(dst_buf, buf, dst_size);
    skip(dst_size);
    return true;
}

size_t slice_istream::read_at_most(void *dst_buf, size_t dst_size) noexcept {
    dst_size = std::min(dst_size, size);
    read_all(dst_buf, dst_size);
    return dst_size;
}

slice_t slice_istream::read_to_delimiter(slice_t delim) noexcept  {
    slice_t found = find(delim);
    if (!found)
        return null_slice;
    slice_t result(buf, found.buf);
    set_start(found.end());
    return result;
}

slice_t slice_istream::read_to_delimiter_or_end(slice_t delim) noexcept {
    slice_t found = find(delim);
    if (found) {
        slice_t result(buf, found.buf);
        set_start(found.end());
        return result;
    } else {
        slice_t result = *this;
        set_start(end());
        return result;
    }
}

slice_t slice_istream::read_bytes_in_set(slice_t set) noexcept {
    const void *next = find_byte_not_in(set);
    if (!next)
        next = end();
    slice_t result(buf, next);
    set_start(next);
    return result;
}

uint8_t slice_istream::read_byte() noexcept {
    if (_usually_false(size == 0))
        return 0;
    uint8_t result = (*this)[0];
    skip(1);
    return result;
}

void slice_istream::skip_to(const void *pos) {
    assert_precondition(pos >= buf && pos <= end());
    set_start(pos);
}

void slice_istream::rewind_to(const void *pos) {
    assert_precondition(pos <= buf);
    set_start(pos);
}

uint64_t slice_istream::read_decimal() noexcept {
    uint64_t n = 0;
    while (size > 0 && isdigit(*(char*)buf)) {
        n = 10*n + (*(char*)buf - '0');
        skip(1);
        if (n > UINT64_MAX/10)
            break;
    }
    return n;
}

int64_t slice_istream::read_signed_decimal() noexcept {
    bool negative = (size > 0 && (*this)[0] == '-');
    if (negative)
        skip(1);
    uint64_t n = read_decimal();
    if (n > INT64_MAX)
        return 0;
    return negative ? -(int64_t)n : (int64_t)n;
}

uint64_t slice_istream::read_hex() noexcept {
    uint64_t n = 0;
    while (size > 0) {
        int digit = _digit_to_int(*(char*)buf);
        if (digit < 0)
            break;
        n = (n <<4 ) + digit;
        skip(1);
        if (n > UINT64_MAX/16)
            break;
    }
    return n;
}

std::optional<uint64_t> slice_istream::read_uvar_int() noexcept {
    uint64_t n;
    if (size_t bytes_read = get_uvar_int(*this, &n); bytes_read > 0) {
        skip(bytes_read);
        return n;
    } else {
        return std::nullopt;
    }
}

std::optional<uint32_t> slice_istream::read_uvar_int32() noexcept {
    uint32_t n;
    if (size_t bytes_read = get_uvar_int32(*this, &n); bytes_read > 0) {
        skip(bytes_read);
        return n;
    } else {
        return std::nullopt;
    }
}

}
