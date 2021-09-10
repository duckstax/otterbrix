#pragma once

#include "slice.hpp"

namespace storage { namespace base64 {

std::string encode(slice_t data);
alloc_slice_t decode(slice_t b64);
slice_t decode(slice_t input, void *output_buffer, size_t size_buffer) noexcept;

} }
