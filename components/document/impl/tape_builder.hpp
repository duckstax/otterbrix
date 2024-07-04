#pragma once

#include <components/document/impl/document.hpp>
#include <components/document/impl/mr_utils.hpp>
#include <components/document/impl/tape_writer.hpp>

namespace components::document {

    template<typename K>
    struct tape_builder {
        using allocator_type = std::pmr::memory_resource;

        tape_builder() noexcept;
        tape_builder(allocator_type* allocator, impl::immutable_document& doc) noexcept;
        tape_builder(allocator_type* allocator, impl::mutable_document& doc) noexcept;

        ~tape_builder();

        tape_builder(tape_builder&&) noexcept;

        tape_builder(const tape_builder&) = delete;

        tape_builder& operator=(tape_builder&&) noexcept;

        tape_builder& operator=(const tape_builder&) = delete;

        void build(std::string_view value) noexcept;

        void build(int8_t value) noexcept;
        void build(int16_t value) noexcept;
        void build(int32_t value) noexcept;
        void build(int64_t value) noexcept;

        void build(int128_t value) noexcept;

        void build(uint8_t value) noexcept;
        void build(uint16_t value) noexcept;
        void build(uint32_t value) noexcept;
        void build(uint64_t value) noexcept;

        void build(float value) noexcept;
        void build(double value) noexcept;
        void build(bool value) noexcept;
        void build(nullptr_t) noexcept;
        void visit_null_atom() noexcept;

    private:
        allocator_type* allocator_;
        impl::tape_writer<K>* tape_;

        void append(uint64_t val, types::physical_type t) noexcept;

    private:
        template<typename T>
        void append2(uint64_t val, T val2, types::physical_type t) noexcept;

        template<typename T>
        void append3(T val2, types::physical_type t) noexcept;
    };

    template<typename K>
    tape_builder<K>::tape_builder() noexcept
        : tape_(nullptr)
        , allocator_(nullptr) {}

    template<typename K>
    tape_builder<K>::~tape_builder() {
        mr_delete(allocator_, static_cast<K*>(tape_));
    }

    template<typename K>
    tape_builder<K>::tape_builder(tape_builder&& other) noexcept
        : allocator_(other.allocator_)
        , tape_(other.tape_) {
        other.allocator_ = nullptr;
        other.tape_ = nullptr;
    }

    template<typename K>
    tape_builder<K>& tape_builder<K>::operator=(tape_builder&& other) noexcept {
        if (this == &other) {
            return *this;
        }
        allocator_ = other.allocator_;
        tape_ = other.tape_;
        other.allocator_ = nullptr;
        other.tape_ = nullptr;
        return *this;
    }
    // struct tape_builder

    template<typename K>
    tape_builder<K>::tape_builder(allocator_type* allocator, impl::immutable_document& doc) noexcept
        : allocator_(allocator)
        , tape_(new (allocator_->allocate(sizeof(impl::tape_writer_to_immutable)))
                    impl::tape_writer_to_immutable(doc)) {}

    template<typename K>
    tape_builder<K>::tape_builder(allocator_type* allocator, impl::mutable_document& doc) noexcept
        : allocator_(allocator)
        , tape_(new (allocator_->allocate(sizeof(impl::tape_writer_to_mutable))) impl::tape_writer_to_mutable(doc)) {}

    template<typename K>
    void tape_builder<K>::build(std::string_view value) noexcept {
        // we advance the point, accounting for the fact that we have a NULL termination
        append(tape_->next_string_buf_index(), types::physical_type::STRING);
        tape_->append_string(value);
    }

    template<typename K>
    void tape_builder<K>::build(int8_t value) noexcept {
        append(value, types::physical_type::INT8);
    }

    template<typename K>
    void tape_builder<K>::build(int16_t value) noexcept {
        append(value, types::physical_type::INT16);
    }

    template<typename K>
    void tape_builder<K>::build(int32_t value) noexcept {
        append(value, types::physical_type::INT32);
    }

    template<typename K>
    void tape_builder<K>::build(int64_t value) noexcept {
        append2(0, value, types::physical_type::INT64);
    }

    template<typename K>
    void tape_builder<K>::build(int128_t value) noexcept {
        append3(value, types::physical_type::INT128);
    }

    template<typename K>
    void tape_builder<K>::build(uint8_t value) noexcept {
        append(value, types::physical_type::UINT8);
    }

    template<typename K>
    void tape_builder<K>::build(uint16_t value) noexcept {
        append(value, types::physical_type::UINT16);
    }

    template<typename K>
    void tape_builder<K>::build(uint32_t value) noexcept {
        append(value, types::physical_type::UINT32);
    }

    template<typename K>
    void tape_builder<K>::build(uint64_t value) noexcept {
        append(0, types::physical_type::UINT64);
        tape_->append(value);
    }

    template<typename K>
    void tape_builder<K>::build(float value) noexcept {
        uint64_t tape_data;
        std::memcpy(&tape_data, &value, sizeof(value));
        append(tape_data, types::physical_type::FLOAT);
    }

    template<typename K>
    void tape_builder<K>::build(double value) noexcept {
        append2(0, value, types::physical_type::DOUBLE);
    }

    template<typename K>
    void tape_builder<K>::build(bool value) noexcept {
        append(0, value ? types::physical_type::BOOL_TRUE : types::physical_type::BOOL_FALSE);
    }

    template<typename K>
    void tape_builder<K>::build(nullptr_t) noexcept {
        visit_null_atom();
    }

    template<typename K>
    void tape_builder<K>::visit_null_atom() noexcept {
        append(0, types::physical_type::NA);
    }

    template<typename K>
    void tape_builder<K>::append(uint64_t val, types::physical_type t) noexcept {
        tape_->append(val | (static_cast<uint64_t>(t) << 56));
    }

    template<typename K>
    template<typename T>
    void tape_builder<K>::append2(uint64_t val, T val2, types::physical_type t) noexcept {
        append(val, t);
        static_assert(sizeof(val2) == sizeof(uint64_t), "Type is not 64 bits!");
        tape_->copy(&val2);
    }

    template<typename K>
    template<typename T>
    void tape_builder<K>::append3(T val2, types::physical_type t) noexcept {
        append(0, t);
        static_assert(sizeof(val2) == 2 * sizeof(uint64_t), "Type is not 128 bits!");
        auto data = reinterpret_cast<uint64_t*>(&val2);
        tape_->copy(data);
        tape_->copy(data + 1);
    }

} // namespace components::document