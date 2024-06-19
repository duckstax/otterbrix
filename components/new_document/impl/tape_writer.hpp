#pragma once

#include <components/new_document/impl/document.hpp>
#include <cstring>
#include <vector>

namespace components::new_document::impl {

    template<typename T>
    class tape_writer {
    public:
        virtual ~tape_writer() = default;

        void append(uint64_t val) noexcept;
        void copy(void* val_ptr) noexcept;

        uint64_t next_string_buf_index() noexcept;
        void append_string(std::string_view val) noexcept;

    private:
        T* self();
    };

    template<typename T>
    void tape_writer<T>::append(uint64_t val) noexcept {
        return self()->append_impl(val);
    }

    template<typename T>
    void tape_writer<T>::copy(void* val_ptr) noexcept {
        return self()->copy_impl(val_ptr);
    }

    template<typename T>
    uint64_t tape_writer<T>::next_string_buf_index() noexcept {
        return self()->next_string_buf_index_impl();
    }

    template<typename T>
    void tape_writer<T>::append_string(std::string_view val) noexcept {
        return self()->append_string_impl(val);
    }

    template<typename T>
    T* tape_writer<T>::self() {
        return static_cast<T*>(this);
    }
    // class tape_writer

    class tape_writer_to_immutable : public tape_writer<tape_writer_to_immutable> {
    public:
        inline explicit tape_writer_to_immutable(impl::immutable_document& doc);
        inline void append_impl(uint64_t val) noexcept;
        inline void copy_impl(void* val_ptr) noexcept;

        inline uint64_t next_string_buf_index_impl() noexcept;
        inline void append_string_impl(std::string_view val) noexcept;

    private:
        impl::immutable_document* doc_{};
        uint8_t*& current_string_buf_loc;
        uint64_t*& next_tape_loc;
    }; // class tape_writer_to_immutable

    class tape_writer_to_mutable : public tape_writer<tape_writer_to_mutable> {
    public:
        inline explicit tape_writer_to_mutable(impl::mutable_document& doc);
        inline void append_impl(uint64_t val) noexcept;
        inline void copy_impl(void* val_ptr) noexcept;

        inline uint64_t next_string_buf_index_impl() noexcept;
        inline void append_string_impl(std::string_view val) noexcept;

    private:
        std::pmr::vector<uint64_t>* tape_ptr;
        std::pmr::vector<uint8_t>* current_string_buf;
    }; // class tape_writer_to_mutable

    inline tape_writer_to_immutable::tape_writer_to_immutable(impl::immutable_document& doc)
        : doc_(&doc)
        , current_string_buf_loc{doc.current_string_buf_loc}
        , next_tape_loc{doc.next_tape_loc} {}

    inline void tape_writer_to_immutable::append_impl(uint64_t val) noexcept {
        *next_tape_loc = val;
        next_tape_loc++;
    }

    inline void tape_writer_to_immutable::copy_impl(void* val_ptr) noexcept {
        memcpy(next_tape_loc, val_ptr, sizeof(uint64_t));
        next_tape_loc++;
    }

    inline uint64_t tape_writer_to_immutable::next_string_buf_index_impl() noexcept {
        return current_string_buf_loc - doc_->string_buf.get();
    }

    void tape_writer_to_immutable::append_string_impl(std::string_view val) noexcept {
        auto str_length = uint32_t(val.size());
        memcpy(current_string_buf_loc, &str_length, sizeof(uint32_t));
        current_string_buf_loc += sizeof(uint32_t);
        memcpy(current_string_buf_loc, val.data(), str_length);
        current_string_buf_loc += str_length;
        *current_string_buf_loc = 0;
        current_string_buf_loc++;
    }

    tape_writer_to_mutable::tape_writer_to_mutable(impl::mutable_document& doc)
        : tape_ptr(&doc.tape)
        , current_string_buf(&doc.string_buf) {}

    void tape_writer_to_mutable::append_impl(uint64_t val) noexcept { tape_ptr->push_back(val); }

    void tape_writer_to_mutable::copy_impl(void* val_ptr) noexcept {
        tape_ptr->push_back({});
        memcpy(&tape_ptr->back(), val_ptr, sizeof(uint64_t));
    }

    uint64_t tape_writer_to_mutable::next_string_buf_index_impl() noexcept { return current_string_buf->size(); }

    void tape_writer_to_mutable::append_string_impl(std::string_view val) noexcept {
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

} // namespace components::new_document::impl