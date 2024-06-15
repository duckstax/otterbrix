#pragma once

#include <components/new_document/base.hpp>
#include <components/new_document/impl/error.h>
#include <components/new_document/impl/tape_ref.h>

#include <memory>
#include <memory_resource>

namespace components::document::impl {

    template<typename T>
    class element;

    template<typename T>
    class array_deleter {
    public:
        array_deleter()
            : allocator_(nullptr)
            , n_(0) {}

        array_deleter(std::pmr::memory_resource* allocator, size_t n)
            : allocator_(allocator)
            , n_(n) {}

        ~array_deleter() = default;

        array_deleter(array_deleter&& other) noexcept
            : allocator_(other.allocator_)
            , n_(other.n_) {
            other.allocator_ = nullptr;
        }

        array_deleter(const array_deleter&) = delete;

        array_deleter& operator=(array_deleter&& other) noexcept {
            allocator_ = other.allocator_;
            n_ = other.n_;
            other.allocator_ = nullptr;
            return *this;
        }

        array_deleter& operator=(const array_deleter&) = delete;

        void operator()(T* p) {
            if (allocator_ != nullptr) {
                std::destroy_n(p, n_);
                allocator_->deallocate(p, n_ * sizeof(T));
            }
        }

    private:
        std::pmr::memory_resource* allocator_;
        size_t n_;
    };

    template<typename T>
    class base_document {
    public:
        virtual ~base_document() = default;
        const uint64_t& get_tape(size_t json_index) const noexcept { return self()->get_tape_impl(json_index); }
        const uint8_t& get_string_buf(size_t json_index) const noexcept {
            return self()->get_string_buf_impl(json_index);
        }
        const uint8_t* get_string_buf_ptr() const noexcept { return self()->get_string_buf_ptr_impl(); }

        size_t size() const noexcept { return self()->size_impl(); }
        std::pmr::string serialize() const noexcept { return self()->serialize_impl(); }

        element<T> next_element() const noexcept { return {internal::tape_ref(this, size())}; }

    private:
        const T* self() const { return static_cast<const T*>(this); }
    }; // class base_document

    class immutable_document : public base_document<immutable_document> {
    public:
        using allocator_type = std::pmr::memory_resource;

        immutable_document() noexcept;

        explicit immutable_document(allocator_type*) noexcept;
        ~immutable_document() noexcept override = default;

        immutable_document(immutable_document&& other) noexcept;
        immutable_document(const immutable_document&) = delete;
        immutable_document& operator=(immutable_document&& other) noexcept;
        immutable_document& operator=(const immutable_document&) = delete;

        const uint64_t& get_tape_impl(size_t json_index) const { return tape[json_index]; }
        const uint8_t& get_string_buf_impl(size_t json_index) const { return string_buf[json_index]; }
        const uint8_t* get_string_buf_ptr_impl() const noexcept { return string_buf.get(); }

        error_code allocate(size_t len) noexcept;

        size_t capacity() const noexcept;
        size_t size_impl() const noexcept { return next_tape_loc - tape.get(); }

        std::pmr::string serialize_impl() const noexcept {
            size_t tape_size = size_impl() * sizeof(uint64_t);
            size_t header = sizeof(binary_type) + sizeof(size_t);
            size_t size = header + tape_size + (current_string_buf_loc - string_buf.get());
            uint8_t* buf = static_cast<uint8_t*>(allocator_->allocate(size));
            *buf = binary_type::tape;
            *reinterpret_cast<size_t*>(buf) = size_impl();
            std::memcpy(buf + header, tape.get(), tape_size);
            std::memcpy(buf + header + tape_size, string_buf.get(), current_string_buf_loc - string_buf.get());
            std::pmr::string res(buf, buf + size, allocator_);
            allocator_->deallocate(buf, size);
            return res;
        }

    private:
        allocator_type* allocator_;
        std::unique_ptr<uint64_t[], array_deleter<uint64_t>> tape{};
        std::unique_ptr<uint8_t[], array_deleter<uint8_t>> string_buf{};
        size_t allocated_capacity{0};

        uint64_t* next_tape_loc = nullptr;
        uint8_t* current_string_buf_loc = nullptr;
        friend class tape_writer_to_immutable;
    }; // class immutable_document

    class mutable_document : public base_document<mutable_document> {
    public:
        using allocator_type = std::pmr::memory_resource;

        mutable_document() noexcept = default;

        explicit mutable_document(allocator_type*) noexcept;

        ~mutable_document() noexcept override = default;

        mutable_document(mutable_document&& other) noexcept;

        mutable_document(const mutable_document&) = delete;

        mutable_document& operator=(mutable_document&& other) noexcept = default;

        mutable_document& operator=(const mutable_document&) = delete;

        const uint64_t& get_tape_impl(size_t json_index) const { return tape[json_index]; }
        const uint8_t& get_string_buf_impl(size_t json_index) const { return string_buf[json_index]; }
        const uint8_t* get_string_buf_ptr_impl() const noexcept { return string_buf.data(); }

        size_t size_impl() const noexcept { return tape.size(); }

        std::pmr::string serialize_impl() const noexcept {
            size_t tape_size = size_impl() * sizeof(uint64_t);
            size_t header = sizeof(binary_type) + sizeof(size_t);
            size_t size = header + tape_size + string_buf.size();
            uint8_t* buf = string_buf.get_allocator().allocate(size);
            *buf = binary_type::tape;
            *reinterpret_cast<size_t*>(buf) = tape.size();
            std::memcpy(buf + header, tape.data(), tape_size);
            std::memcpy(buf + header + tape_size, string_buf.data(), string_buf.size());
            std::pmr::string res(buf, buf + size, string_buf.get_allocator());
            string_buf.get_allocator().deallocate(buf, size);
            return res;
        }

    private:
        std::pmr::vector<uint64_t> tape{};
        std::pmr::vector<uint8_t> string_buf{};
        friend class tape_writer_to_mutable;
    }; // class mutable_document

    template<typename T>
    std::unique_ptr<T[], array_deleter<T>> allocator_make_unique_ptr(std::pmr::memory_resource* allocator, size_t n) {
        T* array = new (allocator->allocate(n * sizeof(T))) T[n];
        return {array, array_deleter<T>(allocator, n)};
    }

} // namespace components::document::impl
