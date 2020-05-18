#pragma once

#include <cstdint>
#include <cstring>

#include <boost/utility/string_view.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>


namespace components {

    template<class Char, class Traits = std::char_traits<Char>>
    using basic_string_view = boost::basic_string_view<Char, Traits>;

    using bytes_view = basic_string_view<uint8_t>;

    class buffer_t : public boost::intrusive_ref_counter<boost::thread_safe_counter> {
    public:

        buffer_t() = delete ;

        buffer_t(const buffer_t &) = delete;

        buffer_t &operator=(const buffer_t &) = delete;

        buffer_t(const uint8_t *data, int64_t size);

        buffer_t(uint8_t* data, int64_t size);

        buffer_t(uintptr_t address, int64_t size);

        explicit buffer_t(boost::string_view data);

        virtual ~buffer_t() = default;

        uint8_t operator[](std::size_t i) const;

        bool equals(const buffer_t &other, int64_t nbytes) const;

        bool operator==(const buffer_t &other) const;

        void zero_padding();

        explicit operator boost::string_view() const;

        explicit operator bytes_view() const;

        const uint8_t *data() const;

        uint8_t *mutable_data();

        uintptr_t address() const;

        uintptr_t mutable_address() const;

        int64_t size() const;

        int64_t capacity() const;

        bool is_mutable() const;

        bool resize( int64_t new_size, bool shrink_to_fit = true);

        bool reserve( int64_t new_capacity);

    protected:

        void check_mutable() const;

        bool is_mutable_;
        const uint8_t *data_;
        uint8_t *mutable_data_;
        int64_t size_;
        int64_t capacity_;
    };

    template <class T>
    bool typed_resize(buffer_t&buffer , int64_t new_nb_elements, bool shrink_to_fit = true) {
        return buffer.resize(sizeof(T) * new_nb_elements, shrink_to_fit);
    }

    template <class T>
    bool typed_reserve(buffer_t&buffer ,const int64_t new_nb_elements) {
        return buffer.reserve(sizeof(T) * new_nb_elements);
    }

    buffer_t copy_slice(buffer_t&buffer, int64_t start,  int64_t nbytes);


}