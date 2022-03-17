#include "writer.hpp"
#include <components/document/support/platform_compat.hpp>
#include <components/document/support/exception.hpp>
#include <components/document/support/better_assert.hpp>
#include <boost/beast/core/detail/base64.hpp>
#include <algorithm>

namespace document {

writer_t::writer_t(size_t initial_capacity)
    : _chunk_size(initial_capacity)
    , _output_file(nullptr)
{
    add_chunk(initial_capacity);
}

writer_t::writer_t(FILE *output_file)
    : writer_t(default_initial_capacity)
{
    assert_precondition(output_file);
    _output_file = output_file;
}

FILE *writer_t::output_file() const {
    return _output_file;
}

writer_t::writer_t(writer_t&& w) noexcept
    : _available(std::move(w._available))
    , _chunks(std::move(w._chunks))
    , _chunk_size(w._chunk_size)
    , _length(w._length)
    , _output_file(w._output_file)
{
    migrate_initial_buf(w);
    memcpy(_initial_buf, w._initial_buf, sizeof(_initial_buf));
    w._output_file = nullptr;
}

writer_t::~writer_t() {
    if (_output_file)
        flush();
    for (auto &chunk : _chunks)
        free_chunk(chunk);
}

writer_t& writer_t::operator= (writer_t&& w) noexcept {
    _available = std::move(w._available);
    _length = w._length;
    _chunks = std::move(w._chunks);
    migrate_initial_buf(w);
    _output_file = w._output_file;
    memcpy(_initial_buf, w._initial_buf, sizeof(_initial_buf));
    w._output_file = nullptr;
    return *this;
}

size_t writer_t::length() const {
    return _length - _available.size;
}

const void *writer_t::write(slice_t s) {
    return _write(s.buf, s.size);
}

const void *writer_t::write(const void *data, size_t length) {
    return _write(data, length);
}

writer_t &writer_t::operator<<(uint8_t byte) {
    _write(&byte,1);
    return *this;
}

void writer_t::pad_to_even_length() {
    if (length() & 1) *this << 0;
}

writer_t &writer_t::operator<<(slice_t s) {
    write(s);
    return *this;
}

void writer_t::_reset() {
    if (_output_file)
        return;

    size_t n_chunks = _chunks.size();
    if (n_chunks > 1) {
        for (size_t i = 0; i < n_chunks-1; i++)
            free_chunk(_chunks[i]);
        _chunks.erase(_chunks.begin(), _chunks.end() - 1);
    }
    _available = _chunks[0];
#if DEBUG
    memset((void*)_available.buf, 0xdd, _available.size);
#endif
}

void writer_t::reset() {
    _reset();
    _length = _available.size;
}

void writer_t::assert_length_correct() const {
#if DEBUG
    if (!_output_file) {
        size_t len = 0;
        foreach_chunk([&](slice_t chunk) {
            len += chunk.size;
        });
        assert_postcondition(len == length());
    }
#endif
}

void* writer_t::_write(const void* data_or_null, size_t length) {
    void* result;
    if (_usually_true(length <= _available.size)) {
        result = (void*)_available.buf;
        if (data_or_null)
            memcpy((void*)_available.buf, data_or_null, length);
        _available.move_start(length);
    } else {
        result = write_to_new_chunk(data_or_null, length);
    }
    assert_length_correct();
    return result;
}

void* writer_t::write_to_new_chunk(const void* data_or_null, size_t length) {
    if (_output_file) {
        flush();
        if (length > _chunk_size) {
            free_chunk(_chunks.back());
            _chunks.clear();
            add_chunk(length);
        }
        _length -= _available.size;
        _available = _chunks[0];
        _length += _available.size;
    } else {
        if (_usually_true(_chunk_size <= 64*1024))
            _chunk_size *= 2;
        add_chunk(std::max(length, _chunk_size));
    }
    auto result = (void*)_available.buf;
    if (data_or_null)
        memcpy((void*)_available.buf, data_or_null, length);
    _available.move_start(length);
    return result;
}

void writer_t::flush() {
    if (!_output_file)
        return;
    auto chunk = _chunks.back();
    size_t written_length = chunk.size - _available.size;
    if (written_length > 0) {
        _length -= _available.size;
        if (fwrite(chunk.buf, 1, written_length, _output_file) < written_length)
            exception_t::_throw_errno("writer_t can't write to file");
        _available = chunk;
        _length += _available.size;
    }
    assert_length_correct();
}

void writer_t::add_chunk(size_t capacity) {
    _length -= _available.size;
    if (!_chunks.empty()) {
        auto &last = _chunks.back();
        last.set_size(last.size - _available.size);
    }
    if (_chunks.empty() && capacity <= default_initial_capacity)
        _available = _chunks.emplace_back(_initial_buf, sizeof(_initial_buf));
    else
        _available = _chunks.emplace_back(slice_t::new_bytes(capacity), capacity);
    _length += _available.size;
}

void writer_t::free_chunk(slice_t chunk) {
    if (chunk.buf != &_initial_buf)
        ::free((void*)chunk.buf);
}

void writer_t::migrate_initial_buf(const writer_t& other) {
    for(auto& chunk : _chunks) {
        if(chunk.buf == other._initial_buf) {
            chunk = slice_t(_initial_buf, chunk.size);
            break;
        }
    }
    slice_t old_initial_buf {other._initial_buf, sizeof(other._initial_buf)};
    if(old_initial_buf.is_contains_address(_available.buf)) {
        const size_t available_offset = old_initial_buf.offsetOf(_available.buf);
        _available = slice_t(_initial_buf, sizeof(_initial_buf));
        _available.move_start(available_offset);
    }
}

std::vector<slice_t> writer_t::output() const {
    std::vector<slice_t> result;
    result.reserve(_chunks.size());
    foreach_chunk([&](slice_t chunk) {
        result.push_back(chunk);
    });
    return result;
}

void writer_t::copy_output_to(void *dst) const {
    foreach_chunk([&](slice_t chunk) {
        chunk.copy_to(dst);
        dst = offsetby(dst, chunk.size);
    });
}

alloc_slice_t writer_t::copy_output() const {
    assert(!_output_file);
    alloc_slice_t output(length());
    copy_output_to((void*)output.buf);
    return output;
}

alloc_slice_t writer_t::finish() {
    alloc_slice_t output;
    if (_output_file) {
        flush();
    } else {
        output = copy_output();
        reset();
    }
    assert_length_correct();
    return output;
}

bool writer_t::write_output_to_file(FILE *f) {
    assert_precondition(!_output_file);
    bool result = true;
    foreach_chunk([&](slice_t chunk) {
        if (result && fwrite(chunk.buf, chunk.size, 1, f) < chunk.size)
            result = false;
    });
    if (result) {
        auto len = length();
        _reset();
        _length = len - _available.size;
    }
    return result;
}

slice_t writer_t::encode_base64(slice_t data) {
    using namespace boost::beast::detail;
    auto dst = new char[base64::encoded_size(data.size)];
    auto size = boost::beast::detail::base64::encode(dst, data.buf, data.size);
    auto res = new char[size];
    strncpy(res, dst, size);
    delete[] dst;
    return slice_t(res, size);
}

slice_t writer_t::decode_base64(slice_t data) {
    using namespace boost::beast::detail;
    auto dst = new char[base64::decoded_size(data.size)];
    auto size = base64::decode(dst, static_cast<const char *>(data.buf), data.size).first;
    auto res = new char[size];
    strncpy(res, dst, size);
    delete[] dst;
    return slice_t(res, size);
}

void writer_t::write_base64(slice_t data) {
    auto base64str = encode_base64(data);
    if (_output_file) {
        auto dst = static_cast<char*>(slice_t::new_bytes(base64str.size));
        strncpy(dst, static_cast<const char*>(base64str.buf), base64str.size);
    } else {
        write(base64str);
        base64str.release();
    }
}

void writer_t::write_decoded_base64(slice_t base64str) {
    auto data = decode_base64(base64str);
    write(data);
    data.release();
}

void *writer_t::reserve_space(size_t length) {
    return _write(nullptr, length);
}

}
