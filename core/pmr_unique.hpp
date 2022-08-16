#pragma once

#include <memory>
#include <memory_resource>
#include <type_traits>

namespace core {

    class deleter final {
    public:
        explicit deleter(std::pmr::memory_resource* ptr)
            : ptr_(ptr) {}

        template<class T>
        void operator()(T* target) {
            target->~T();
            ptr_->deallocate(target, sizeof(T));
        }

    private:
        std::pmr::memory_resource* ptr_;
    };

    namespace pmr {
        template<class T>
        using unique_ptr = std::unique_ptr<T, deleter>;

        template<class T>
        struct make_uniq_ {
            typedef unique_ptr<T> single_object_;
        };

        template<class T>
        struct make_uniq_<T[]> {
            typedef unique_ptr<T[]> array_;
        };

        template<class T, size_t _Bound>
        struct make_uniq_<T[_Bound]> {
            struct invalid_type_ {};
        };

        template<class T, class... Args>
        inline typename make_uniq_<T>::__single_object
        make_unique(std::pmr::memory_resource* ptr, Args&&... args) {
            return unique_ptr<T>(new T(std::forward<Args>(args)...), deleter(ptr));
        }

        template<typename T>
        inline typename make_uniq_<T>::array_
        make_unique(std::pmr::memory_resource* ptr, size_t num) {
            return unique_ptr<T>(new std::remove_extent_t<T>[num](), deleter(ptr));
        }

        template<typename _Tp, typename... Args>
        inline typename make_uniq_<_Tp>::invalid_type_
        make_unique(std::pmr::memory_resource*, Args&&...) = delete;

    } // namespace pmr

} // namespace core