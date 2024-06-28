#pragma once
#include <components/new_document/base.hpp>
#include <components/new_document/impl/document.hpp>
#include <components/new_document/impl/element.hpp>
#include <components/new_document/impl/tape_builder.hpp>
#include <components/types/types.hpp>
#include <cstdint>
#include <string>

namespace components::new_document {

    // TODO: support dict and arrays
    class value_t {
    public:
        template<typename T>
        explicit value_t(std::pmr::memory_resource* allocator, T value);
        explicit value_t(std::pmr::memory_resource* allocator);

        template<typename T>
        void set(T value);

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
        impl::mutable_document* mut_src_;
        tape_builder<impl::tape_writer_to_mutable> builder_{};
    };

    std::string to_string(const value_t& value);

    value_t sum(const value_t& value1, const value_t& value2);

    template<typename T>
    value_t::value_t(std::pmr::memory_resource* allocator, T value)
        : mut_src_(new (allocator->allocate(sizeof(impl::mutable_document))) impl::mutable_document(allocator)) {
        builder_ = components::new_document::tape_builder<impl::tape_writer_to_mutable>(allocator, *mut_src_);
        element_ = mut_src_->next_element();
        builder_.build(value);
    }

    template<typename T>
    void value_t::set(T value) {
        element_ = mut_src_->next_element();
        builder_.build(value);
    }

    template<class T>
    T value_t::as() const {
        return element_.get<T>();
    }

} // namespace components::new_document