#pragma once

#include <cassert>
#include <cstddef>

namespace core {

    class static_resource final : public std::pmr::memory_resource {
        void* p_;
        std::size_t n_;
        std::size_t size_;

    public:

        static_resource(static_resource const&) = delete;
        static_resource& operator=(static_resource const&) = delete;
        ~static_resource() noexcept;
        static_resource(unsigned char* buffer,std::size_t size) noexcept;

        static_resource(std::byte* buffer,std::size_t size) noexcept
            : static_resource(reinterpret_cast<unsigned char*>(buffer),size) {
        }


        template<std::size_t N>
        explicit static_resource(unsigned char (&buffer)[N]) noexcept
            : static_resource(&buffer[0], N) {
        }


        template<std::size_t N>
        explicit static_resource(std::byte (&buffer)[N]) noexcept
            : static_resource(&buffer[0], N) {
        }



        template<std::size_t N>
        static_resource(
            unsigned char (&buffer)[N], std::size_t n) noexcept
            : static_resource(&buffer[0], n) {

            assert(n <= N);
        }


        template<std::size_t N>
        static_resource(std::byte (&buffer)[N], std::size_t n) noexcept
            : static_resource(&buffer[0], n) {

            assert(n <= N);
        }

        void release() noexcept;

    protected:
        void* do_allocate(std::size_t n,std::size_t align) override;
        void do_deallocate(void* p,std::size_t n,std::size_t align) override;
        bool do_is_equal( std::pmr::memory_resource const& mr) const noexcept override;
    };

    static_resource::~static_resource() noexcept = default;
    static_resource::static_resource(
            unsigned char* buffer,
            std::size_t size) noexcept
        : p_(buffer)
        , n_(size)
        , size_(size){
    }

    void static_resource::release() noexcept {
        p_ = reinterpret_cast<char*>(p_) - (size_ - n_);
        n_ = size_;
    }

    void* static_resource::do_allocate(std::size_t n,std::size_t align){
        auto p = alignment::align(align, n, p_, n_);
        if(! p) {
            throw std::bad_alloc();
        }
        p_ = reinterpret_cast<char*>(p) + n;
        n_ -= n;
        return p;
    }

    void static_resource::do_deallocate(
            void*,
            std::size_t,
            std::size_t)
    {
        // do nothing
    }

    bool static_resource::do_is_equal(std::pmr:memory_resource const& mr) const noexcept {
        return this == &mr;
    }


    template<>
    struct is_deallocate_trivial<static_resource> {
        static constexpr bool value = true;
    };

} // namespace core