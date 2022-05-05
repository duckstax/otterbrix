#include "spinlock.hpp"

#include <limits>

inline long min_long() {
    return std::numeric_limits<long>::min();
}

void spin_lock::lock() {
    while (lock_.test_and_set(std::memory_order_acquire)) continue;
}
void spin_lock::unlock() {
    lock_.clear(std::memory_order_release);
}

template<class T>
bool cas_weak(std::atomic<T>* obj, T* expected, T desired) {
    return std::atomic_compare_exchange_weak(obj, expected, desired);
}

shared_spinlock::shared_spinlock()
    : flag_(0) {}

void shared_spinlock::lock() {
    long v = flag_.load();
    for (;;) {
        if (v != 0) {
            v = flag_.load();
        } else if (cas_weak(&flag_, &v, min_long())) {
            return;
        }
        // else: next iteration
    }
}

void shared_spinlock::lock_upgrade() {
    lock_shared();
}

void shared_spinlock::unlock_upgrade() {
    unlock_shared();
}

void shared_spinlock::unlock_upgrade_and_lock() {
    unlock_shared();
    lock();
}

void shared_spinlock::unlock_and_lock_upgrade() {
    unlock();
    lock_upgrade();
}

void shared_spinlock::unlock() {
    flag_.store(0);
}

bool shared_spinlock::try_lock() {
    long v = flag_.load();
    return (v == 0) ? cas_weak(&flag_, &v, min_long()) : false;
}

void shared_spinlock::lock_shared() {
    long v = flag_.load();
    for (;;) {
        if (v < 0) {
            // std::this_thread::yield();
            v = flag_.load();
        } else if (cas_weak(&flag_, &v, v + 1)) {
            return;
        }
        // else: next iteration
    }
}

void shared_spinlock::unlock_shared() {
    flag_.fetch_sub(1);
}

bool shared_spinlock::try_lock_shared() {
    long v = flag_.load();
    return (v >= 0) ? cas_weak(&flag_, &v, v + 1) : false;
}
