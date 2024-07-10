#pragma once

#include <components/document/impl/document.hpp>
#include <components/document/impl/mr_utils.hpp>
#include <components/document/impl/tape_writer.hpp>
#include <components/types/types.hpp>

namespace components::document {

    struct tape_builder {
        using allocator_type = std::pmr::memory_resource;

        tape_builder() noexcept;
        tape_builder(impl::base_document& doc) noexcept;

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
        impl::tape_writer tape_;

        void append(uint64_t val, types::physical_type t) noexcept;

        template<typename T>
        void append2(uint64_t val, T val2, types::physical_type t) noexcept;

        template<typename T>
        void append3(T val2, types::physical_type t) noexcept;
    };

    template<typename T>
    void tape_builder::append2(uint64_t val, T val2, types::physical_type t) noexcept {
        append(val, t);
        static_assert(sizeof(val2) == sizeof(uint64_t), "Type is not 64 bits!");
        tape_.copy(&val2);
    }

    template<typename T>
    void tape_builder::append3(T val2, types::physical_type t) noexcept {
        append(0, t);
        static_assert(sizeof(val2) == 2 * sizeof(uint64_t), "Type is not 128 bits!");
        auto data = reinterpret_cast<uint64_t*>(&val2);
        tape_.copy(data);
        tape_.copy(data + 1);
    }

} // namespace components::document