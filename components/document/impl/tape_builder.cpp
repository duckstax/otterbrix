#include "tape_builder.hpp"

namespace components::document {

    tape_builder::tape_builder() noexcept
        : tape_(nullptr)
        , allocator_(nullptr) {}

    tape_builder::~tape_builder() { mr_delete(allocator_, tape_); }

    tape_builder::tape_builder(tape_builder&& other) noexcept
        : allocator_(other.allocator_)
        , tape_(other.tape_) {
        other.allocator_ = nullptr;
        other.tape_ = nullptr;
    }

    tape_builder& tape_builder::operator=(tape_builder&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        allocator_ = other.allocator_;
        tape_ = other.tape_;
        other.allocator_ = nullptr;
        other.tape_ = nullptr;
        return *this;
    }

    tape_builder::tape_builder(allocator_type* allocator, impl::base_document& doc) noexcept
        : allocator_(allocator)
        , tape_(new (allocator_->allocate(sizeof(impl::tape_writer))) impl::tape_writer(doc)) {}

    void tape_builder::build(std::string_view value) noexcept {
        // we advance the point, accounting for the fact that we have a NULL termination
        append(tape_->next_string_buf_index(), types::physical_type::STRING);
        tape_->append_string(value);
    }

    void tape_builder::build(int8_t value) noexcept { append(value, types::physical_type::INT8); }

    void tape_builder::build(int16_t value) noexcept { append(value, types::physical_type::INT16); }

    void tape_builder::build(int32_t value) noexcept { append(value, types::physical_type::INT32); }

    void tape_builder::build(int64_t value) noexcept { append2(0, value, types::physical_type::INT64); }

    void tape_builder::build(int128_t value) noexcept { append3(value, types::physical_type::INT128); }

    void tape_builder::build(uint8_t value) noexcept { append(value, types::physical_type::UINT8); }

    void tape_builder::build(uint16_t value) noexcept { append(value, types::physical_type::UINT16); }

    void tape_builder::build(uint32_t value) noexcept { append(value, types::physical_type::UINT32); }

    void tape_builder::build(uint64_t value) noexcept {
        append(0, types::physical_type::UINT64);
        tape_->append(value);
    }

    void tape_builder::build(float value) noexcept {
        uint64_t tape_data;
        std::memcpy(&tape_data, &value, sizeof(value));
        append(tape_data, types::physical_type::FLOAT);
    }

    void tape_builder::build(double value) noexcept { append2(0, value, types::physical_type::DOUBLE); }

    void tape_builder::build(bool value) noexcept {
        append(0, value ? types::physical_type::BOOL_TRUE : types::physical_type::BOOL_FALSE);
    }

    void tape_builder::build(nullptr_t) noexcept { visit_null_atom(); }

    void tape_builder::visit_null_atom() noexcept { append(0, types::physical_type::NA); }

    void tape_builder::append(uint64_t val, types::physical_type t) noexcept {
        tape_->append(val | (static_cast<uint64_t>(t) << 56));
    }

} // namespace components::document