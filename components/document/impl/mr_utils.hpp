#pragma once

#include <memory_resource>

template<typename T>
void mr_delete(std::pmr::memory_resource* allocator, T* p) {
    if (p == nullptr) {
        return;
    }
    p->~T();
    allocator->deallocate(p, sizeof(T));
}