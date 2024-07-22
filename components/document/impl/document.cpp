#include "document.hpp"

#include "element.hpp"

namespace components::document::impl {

    base_document::base_document(base_document::allocator_type* allocator)
        : tape_(allocator)
        , string_buf_(allocator) {}

    base_document::base_document(base_document&& other) noexcept
        : tape_(std::move(other.tape_))
        , string_buf_(std::move(other.string_buf_)) {}

    const uint64_t& base_document::get_tape(size_t json_index) const noexcept { return tape_[json_index]; }
    const uint8_t& base_document::get_string_buf(size_t json_index) const noexcept { return string_buf_[json_index]; }
    const uint8_t* base_document::get_string_buf_ptr() const noexcept { return string_buf_.data(); }
    const uint64_t* base_document::get_tape_ptr() const noexcept { return tape_.data(); }

    size_t base_document::size() const noexcept { return tape_.size(); }
    size_t base_document::string_buf_size() const noexcept { return string_buf_.size(); }

    element base_document::next_element() const noexcept { return {internal::tape_ref(this, size())}; }

} // namespace components::document::impl
