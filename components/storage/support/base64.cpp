#include "base64.hpp"
#include "encode.hpp"
#include "decode.hpp"
#include "better_assert.hpp"

namespace storage { namespace base64 {

std::string encode(slice_t data) {
    std::string str;
    size_t str_len = ((data.size + 2) / 3) * 4;
    str.resize(str_len);
    char *dst = &str[0];
    ::base64::encoder_t enc;
    enc.set_chars_per_line(0);
    size_t written = enc.encode(data.buf, data.size, dst);
    written += enc.encode_end(dst + written);
    assert(written == str_len);
    (void)written;
    return str;
}

alloc_slice_t decode(slice_t b64) {
    size_t expected_len = ((b64.size + 3) / 4 * 3);
    alloc_slice_t result(expected_len);
    slice_t decoded = decode(b64, (void*)result.buf, result.size);
    if (decoded.size == 0)
        return null_slice;
    assert(decoded.size <= expected_len);
    result.resize(decoded.size);
    return result;
}

slice_t decode(slice_t b64, void *output_buffer, size_t size_buffer) noexcept {
    size_t expected_len = (b64.size + 3) / 4 * 3;
    if (expected_len > size_buffer)
        return null_slice;
    ::base64::decoder_t dec;
    size_t len = dec.decode(b64.buf, b64.size, output_buffer);
    assert(len <= size_buffer);
    return slice_t(output_buffer, len);
}

} }
