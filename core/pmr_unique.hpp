#pragma once

#include <memory>
#include <memory_resource>
#include <type_traits>

namespace core::pmr {

    class deleter_t final {
    public:
        explicit deleter_t(std::pmr::memory_resource* ptr)
            : ptr_(ptr) {}

        template<class T>
        void operator()(T* target) {
            target->~T();
            ptr_->deallocate(target, sizeof(T));
        }

    private:
        std::pmr::memory_resource* ptr_;
    };

    template<class T>
    using unique_ptr = std::unique_ptr<T, deleter_t>;

    template<class Target,class ...Args>
    unique_ptr<Target> make_unique(std::pmr::memory_resource* ptr,Args&&... args){
        auto size = sizeof(Target);
        auto align = alignof(Target);
        auto* buffer = ptr->allocate(size, align);
        auto* target_ptr = new (buffer) Target(ptr,std::forward<Args>(args)...);
        return {target_ptr,deleter_t(ptr)};
    }

} // namespace pmr