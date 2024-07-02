#pragma once
#include <components/document/base.hpp>
#include <components/document/impl/document.hpp>
#include <components/document/impl/element.hpp>
#include <components/document/impl/tape_builder.hpp>
#include <components/types/types.hpp>
#include <cstdint>
#include <string>

namespace components::new_document {

    // TODO: support dict and arrays
    class value_t {
    public:
        template<typename T>
        explicit value_t(std::pmr::memory_resource* allocator, impl::mutable_document* tape, T value);

        void set(impl::element<impl::mutable_document> element);

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
        float as_float() const noexcept;
        double as_double() const noexcept;
        std::string_view as_string() const noexcept;
        // value_t* as_array() const noexcept;
        // value_t* as_dict() const noexcept;

        template<class T>
        T as() const;

        bool is_int() const noexcept;
        bool is_unsigned() const noexcept;
        bool is_double() const noexcept;
        bool is_string() const noexcept;
        // bool is_array() const noexcept;
        // bool is_dict() const noexcept;

    private:
        impl::element<impl::mutable_document> element_;
    };

    std::string to_string(const value_t& value);

    value_t sum(const value_t& value1, const value_t& value2);

    template<typename T>
    value_t::value_t(std::pmr::memory_resource* allocator, impl::mutable_document* tape, T value) {
        auto builder = components::new_document::tape_builder<impl::tape_writer_to_mutable>(allocator, *tape);
        element_ = tape->next_element();
        builder.build(value);
    }

    template<class T>
    T value_t::as() const {
        return element_.get<T>();
    }

} // namespace components::new_document