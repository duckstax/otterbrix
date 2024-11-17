#include "tape_builder.hpp"

namespace components::document {

    tape_builder::tape_builder() noexcept
        : tape_() {}

    tape_builder::tape_builder(tape_builder&& other) noexcept
        : tape_(std::move(other.tape_)) {}

    tape_builder& tape_builder::operator=(tape_builder&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        tape_ = std::move(other.tape_);
        return *this;
    }

    tape_builder::tape_builder(impl::base_document& doc) noexcept
        : tape_(doc) {}

    void tape_builder::build(const std::string& value) {
        // we advance the point, accounting for the fact that we have a NULL termination
        append(tape_.next_string_buf_index(), types::physical_type::STRING);
        tape_.append_string(value);
    }

    void tape_builder::build(const std::pmr::string& value) {
        // we advance the point, accounting for the fact that we have a NULL termination
        append(tape_.next_string_buf_index(), types::physical_type::STRING);
        tape_.append_string(value);
    }

    void tape_builder::visit_null_atom() noexcept { append(0, types::physical_type::NA); }

    void tape_builder::append(uint64_t val, types::physical_type t) noexcept {
        tape_.append(val | (static_cast<uint64_t>(t) << 56));
    }

} // namespace components::document