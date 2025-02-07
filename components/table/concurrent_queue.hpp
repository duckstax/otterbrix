#pragma once
#include <queue>

namespace components::table {

    template<typename T>
    class concurrent_queue_t {
    public:
        concurrent_queue_t() = default;
        explicit concurrent_queue_t(size_t capacity) { q_.reserve(capacity); }

        template<typename U>
        bool enqueue(U&& item) {
            q_.push(std::forward<U>(item));
            return true;
        }
        bool try_dequeue(T& item) {
            if (q_.empty()) {
                return false;
            }
            item = std::move(q_.front());
            q_.pop();
            return true;
        }
        size_t size_approx() const { return q_.size(); }
        template<typename It>
        size_t try_dequeue_bulk(It itemFirst, size_t max) {
            for (size_t i = 0; i < max; i++) {
                if (!try_dequeue(*itemFirst)) {
                    return i;
                }
                itemFirst++;
            }
            return max;
        }

    private:
        std::queue<T, std::deque<T>> q_;
    };

} // namespace components::table