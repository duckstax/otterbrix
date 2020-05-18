#include "buffer.hpp"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <iostream>
#include <cassert>


namespace components {

    constexpr int64_t round_up_to_power_of_2(int64_t value, int64_t factor) {
        return (value + (factor - 1)) & ~(factor - 1);
    }

    constexpr int64_t round_up_to_multiple_of_64(int64_t num) {
        return round_up_to_power_of_2(num, 64);
    }

    template <typename T>
    T* allocate(int64_t new_capacity) {
        return new T[new_capacity];
    }

    template <typename T>
    void reallocate(int64_t old_size, int64_t new_size, uint8_t* ptr) {
        uint8_t* out = nullptr;
        //Allocate<T>(new_size, &out);
        out = allocate<T>(new_size);
        memcpy(out, ptr, static_cast<size_t>(std::min(new_size, old_size)));
        delete[]  ptr;
        *ptr = *out;
   }

    bool buffer_t::equals(const buffer_t& other, const int64_t nbytes) const {
        return this == &other || (size_ >= nbytes && other.size_ >= nbytes &&
                                  (data_ == other.data_ ||
                                   !memcmp(data_, other.data_, static_cast<size_t>(nbytes))));
    }

    bool buffer_t::operator==(const buffer_t& other) const {
        return this == &other || (size_ == other.size_ &&
                                  (data_ == other.data_ ||
                                   !memcmp(data_, other.data_, static_cast<size_t>(size_))));
    }


    void buffer_t::check_mutable() const { assert(is_mutable()); }


    buffer_t::buffer_t(boost::string_view data): buffer_t(reinterpret_cast<const uint8_t *>(data.data()),static_cast<int64_t>(data.size())) {}


    void buffer_t::zero_padding() {
#ifndef NDEBUG
        check_mutable();
#endif
        // A zero-capacity buffer can have a null data pointer
        if (capacity_ != 0) {
            memset(mutable_data_ + size_, 0, static_cast<size_t>(capacity_ - size_));
        }
    }

    buffer_t::operator boost::string_view() const {
        return boost::string_view(reinterpret_cast<const char *>(data_), static_cast<unsigned long>(size_));
    }

    buffer_t::operator bytes_view() const { return bytes_view(data_, static_cast<unsigned long>(size_)); }

    const uint8_t *buffer_t::data() const {
        return data_ ;
    }

    uint8_t *buffer_t::mutable_data() {
#ifndef NDEBUG
        check_mutable();
#endif
        return  mutable_data_ ;
    }

    uintptr_t buffer_t::mutable_address() const {
#ifndef NDEBUG
        check_mutable();
#endif
        return reinterpret_cast<uintptr_t>(mutable_data_);
    }

    int64_t buffer_t::size() const { return size_; }

    bool buffer_t::is_mutable() const { return is_mutable_; }

    uintptr_t buffer_t::address() const { return reinterpret_cast<uintptr_t>(data_); }

    int64_t buffer_t::capacity() const { return capacity_; }

    uint8_t buffer_t::operator[](std::size_t i) const { return data_[i]; }

    buffer_t::buffer_t(const uint8_t *data, int64_t size)
            : is_mutable_(false)
            , data_(data)
            , mutable_data_(nullptr)
            , size_(size)
            , capacity_(size) {
    }

    buffer_t::buffer_t(uintptr_t address, int64_t size): buffer_t(reinterpret_cast<const uint8_t*>(address), size){}

    bool buffer_t::reserve(const int64_t capacity)  {
        if (capacity < 0) {
            std::cerr<<"Negative buffer capacity: "<< capacity << std::endl;
            return false;
        }
        if (!mutable_data_ || capacity > capacity_) {
            uint8_t* new_data = nullptr;
            int64_t new_capacity = round_up_to_multiple_of_64(capacity);
            if (mutable_data_) {
                reallocate<uint8_t>(capacity_, new_capacity, mutable_data_);
                //Reallocate<uint8_t>(capacity_, new_capacity, &mutable_data_);
            } else {
                //Allocate<uint8_t>(new_capacity, &new_data);
                new_data = allocate<uint8_t>(new_capacity);
                mutable_data_ = new_data;
            }
            data_ = mutable_data_;
            capacity_ = new_capacity;
        }
        return true;
    }

    bool buffer_t::resize( int64_t new_size, bool shrink_to_fit )  {

        if (new_size < 0) {
            std::cerr << "Negative buffer resize: " << new_size << std::endl;
            return false;
        }

        if (mutable_data_ && shrink_to_fit && new_size <= size_) {
            int64_t new_capacity = round_up_to_multiple_of_64(new_size);
            if (capacity_ != new_capacity) {
                reallocate<uint8_t>(capacity_, new_capacity, mutable_data_);
                //reallocate<uint8_t>(capacity_, new_capacity, &mutable_data_);
                data_ = mutable_data_;
                capacity_ = new_capacity;
            }
        } else {
            reserve(new_size);
        }
        size_ = new_size;

        return true;
    }

    buffer_t::buffer_t(uint8_t *data, int64_t size)
        : is_mutable_(true)
        , data_(data)
        , mutable_data_ (data)
        , size_(size)
        , capacity_(size) {

    }

    void  copy_slice(buffer_t&buffer,buffer_t&new_buffer,  int64_t start, int64_t nbytes)  {
        assert(start == buffer.size());
        assert(nbytes == buffer.size() - start);
        assert(nbytes != 0);

        new_buffer.resize(nbytes);
        std::memcpy(new_buffer.mutable_data(), buffer.data() + start, static_cast<size_t>(nbytes));
    }


}