#include "tape_writer.hpp"

namespace components::document::impl {

    tape_writer::tape_writer(impl::base_document& doc)
        : tape_ptr(&doc.tape_)
        , current_string_buf(&doc.string_buf_) {}

    void tape_writer::append(uint64_t val) noexcept {
        tape_ptr->push_back(val);
        ;
    }

    void tape_writer::copy(void* val_ptr) noexcept {
        tape_ptr->push_back({});
        memcpy(&tape_ptr->back(), val_ptr, sizeof(uint64_t));
    }

    uint64_t tape_writer::next_string_buf_index() noexcept { return current_string_buf->size(); }

    void tape_writer::append_string(std::string_view val) noexcept {
        auto str_length = static_cast<uint32_t>(val.size());
        auto final_size = sizeof(uint32_t) + str_length + 1;
        auto buf_size = current_string_buf->size();
        current_string_buf->resize(buf_size + final_size);
        auto size_ptr = reinterpret_cast<uint32_t*>(current_string_buf->data() + buf_size);
        std::memcpy(size_ptr, &str_length, sizeof(uint32_t));
        char* str_ptr = reinterpret_cast<char*>(size_ptr + 1);
        std::memcpy(str_ptr, val.data(), str_length);
        str_ptr[str_length] = '\0';
    }

} // namespace components::document::impl