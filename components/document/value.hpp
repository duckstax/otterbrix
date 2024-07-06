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
        explicit value_t(std::pmr::memory_resource* allocator, impl::mutable_document* tape, T value);
        explicit value_t(impl::element<impl::mutable_document> element);
        explicit value_t(impl::element<impl::immutable_document> element);
        explicit value_t() = default;

        void set(impl::element<impl::mutable_document> element);
        void set(impl::element<impl::immutable_document> element);

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

        bool is_immut() const noexcept;
        bool is_mut() const noexcept;

        const impl::element<impl::immutable_document>* get_immut() const noexcept;
        const impl::element<impl::mutable_document>* get_mut() const noexcept;

    private:
        std::string_view get_string_bytes() const noexcept;

        std::variant<impl::element<impl::mutable_document>, impl::element<impl::immutable_document>> element_;
    };

    std::string to_string(const value_t& value);

    value_t sum(const value_t& value1,
                const value_t& value2,
                impl::mutable_document* tape,
                std::pmr::memory_resource* resource);

    template<typename T>
    value_t::value_t(std::pmr::memory_resource* allocator, impl::mutable_document* tape, T value) {
        auto builder = components::document::tape_builder<impl::tape_writer_to_mutable>(allocator, *tape);
        element_ = tape->next_element();
        builder.build(value);
    }

    template<class T>
    T value_t::as() const {
        return std::visit([](auto&& doc) -> T { return doc.template get<T>(); }, element_);
    }

} // namespace components::document