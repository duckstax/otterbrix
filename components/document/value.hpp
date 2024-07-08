#pragma once
#include <components/document/base.hpp>
#include <components/document/impl/document.hpp>
#include <components/document/impl/element.hpp>
#include <components/document/impl/tape_builder.hpp>
#include <components/types/types.hpp>
#include <cstdint>
#include <string>
#include <variant>

namespace components::document {

    // TODO: support dict and arrays
    class value_t {
    public:
        template<typename T>
        explicit value_t(std::pmr::memory_resource* allocator, impl::base_document* tape, T value);
        explicit value_t(impl::element element);
        explicit value_t() = default;

        void set(impl::element element);

        types::physical_type physical_type() const noexcept;
        types::logical_type logical_type() const noexcept;

        bool operator==(const value_t& rhs) const;
        bool operator<(const value_t& rhs) const;
        bool operator>(const value_t& rhs) const { return rhs < *this; }
        bool operator<=(const value_t& rhs) const { return !(*this > rhs); }
        bool operator>=(const value_t& rhs) const { return !(*this < rhs); }
        bool operator!=(const value_t& rhs) const { return !(*this == rhs); }

        bool as_bool() const noexcept;
        int64_t as_int() const noexcept;
        uint64_t as_unsigned() const noexcept;
        int128_t as_int128() const noexcept;
        float as_float() const noexcept;
        double as_double() const noexcept;
        std::string_view as_string() const noexcept;
        // value_t* as_array() const noexcept;
        // value_t* as_dict() const noexcept;

        template<class T>
        T as() const;

        bool is_bool() const noexcept;
        bool is_int() const noexcept;
        bool is_unsigned() const noexcept;
        bool is_int128() const noexcept;
        bool is_double() const noexcept;
        bool is_string() const noexcept;
        // bool is_array() const noexcept;
        // bool is_dict() const noexcept;

        explicit operator bool() const;

        const impl::element* get_element() const noexcept;

    private:
        std::string_view get_string_bytes() const noexcept;

        impl::element element_;
    };

    std::string to_string(const value_t& value);

    value_t
    sum(const value_t& value1, const value_t& value2, impl::base_document* tape, std::pmr::memory_resource* resource);

    template<typename T>
    value_t::value_t(std::pmr::memory_resource* allocator, impl::base_document* tape, T value) {
        auto builder = tape_builder(allocator, *tape);
        element_ = tape->next_element();
        builder.build(value);
    }

    template<class T>
    T value_t::as() const {
        return element_.get<T>();
        ;
    }

} // namespace components::document