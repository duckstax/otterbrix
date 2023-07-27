#pragma once

#include <memory>
#include <boost/container/pmr/memory_resource.hpp>
#include <boost/container/pmr/vector.hpp>
#include <boost/container/pmr/map.hpp>
#include <type_traits>

namespace core::pmr {

    using boost::container::pmr::memory_resource;
    using boost::container::pmr::polymorphic_allocator;

    template <class T>
    using vector = boost::container::pmr::vector<T>;

    template <class Key
             ,class Value
             ,class Compare  = std::less<Key>
             ,class Options = void >
    using map = boost::container::pmr::map<Key,Value,Compare,Options>;

    class deleter_t final {
    public:
        explicit deleter_t(memory_resource* ptr)
            : ptr_(ptr) {}

        template<class T>
        void operator()(T* target) {
            target->~T();
            ptr_->deallocate(target, sizeof(T));
        }

    private:
        memory_resource* ptr_;
    };

    template<class T>
    using unique_ptr = std::unique_ptr<T, deleter_t>;

    template<class Target, class... Args>
    unique_ptr<Target> make_unique(memory_resource* ptr, Args&&... args) {
        auto size = sizeof(Target);
        auto align = alignof(Target);
        auto* buffer = ptr->allocate(size, align);
        auto* target_ptr = new (buffer) Target(ptr, std::forward<Args>(args)...);
        return {target_ptr, deleter_t(ptr)};
    }

    template<class Target, class... Args>
    Target* allocate_ptr(memory_resource* ptr, Args&&... args) {
        auto size = sizeof(Target);
        auto align = alignof(Target);
        auto* buffer = ptr->allocate(size, align);
        auto* target_ptr = new (buffer) Target(ptr, std::forward<Args>(args)...);
        return target_ptr;
    }

    template<class Target>
    void deallocate_ptr(memory_resource* ptr, Target* target) {
        target->~T();
        ptr->deallocate(target, sizeof(Target));
    }

} // namespace core::pmr