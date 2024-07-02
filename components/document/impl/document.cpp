#include "document.hpp"

#include <iomanip>

namespace components::new_document::impl {

    size_t immutable_document::capacity() const noexcept { return allocated_capacity; }

    constexpr size_t align_to(size_t value, size_t target) { return (value + (target - 1)) & ~(target - 1); }

    error_code immutable_document::allocate(size_t capacity) noexcept {
        constexpr size_t padding = 64;
        if (capacity == 0) {
            string_buf.reset();
            tape.reset();
            allocated_capacity = 0;
            return SUCCESS;
        }

        size_t tape_capacity = align_to(capacity + 3, padding);
        // a document with only zero-length strings... could have capacity/3 string
        // and we would need capacity/3 * 5 bytes on the string buffer
        size_t string_capacity = align_to(5 * capacity / 3 + padding, padding);
        try {
            string_buf = allocator_make_unique_ptr<uint8_t>(allocator_, string_capacity);
            tape = allocator_make_unique_ptr<uint64_t>(allocator_, tape_capacity);
            next_tape_loc = tape.get();
            current_string_buf_loc = string_buf.get();
        } catch (std::bad_alloc&) {
            allocated_capacity = 0;
            string_buf.reset();
            tape.reset();
            return MEMALLOC;
        }
        // Technically the allocated_capacity might be larger than capacity
        // so the next line is pessimistic.
        allocated_capacity = capacity;
        return SUCCESS;
    }

    immutable_document::immutable_document() noexcept
        : allocator_(nullptr) {}

    immutable_document::immutable_document(immutable_document::allocator_type* allocator) noexcept
        : allocator_(allocator) {}

    immutable_document::immutable_document(immutable_document&& other) noexcept
        : allocator_(other.allocator_)
        , tape(std::move(other.tape))
        , string_buf(std::move(other.string_buf))
        , next_tape_loc(other.next_tape_loc)
        , current_string_buf_loc(other.current_string_buf_loc) {
        other.allocator_ = nullptr;
        other.next_tape_loc = nullptr;
        other.current_string_buf_loc = nullptr;
    }

    immutable_document& immutable_document::operator=(immutable_document&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        allocator_ = other.allocator_;
        tape = std::move(other.tape);
        string_buf = std::move(other.string_buf);
        next_tape_loc = other.next_tape_loc;
        current_string_buf_loc = other.current_string_buf_loc;
        other.allocator_ = nullptr;
        other.next_tape_loc = nullptr;
        other.current_string_buf_loc = nullptr;
        return *this;
    }

    mutable_document::mutable_document(mutable_document::allocator_type* allocator) noexcept
        : tape(allocator)
        , string_buf(allocator) {}

    mutable_document::mutable_document(mutable_document&& other) noexcept
        : tape(std::move(other.tape))
        , string_buf(std::move(other.string_buf)) {}

} // namespace components::new_document::impl
