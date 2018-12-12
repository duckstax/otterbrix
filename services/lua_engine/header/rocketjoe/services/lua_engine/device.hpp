#pragma once
#include <unordered_map>
#include <thread>
#include <forward_list>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>

namespace rocketjoe { namespace services { namespace lua_engine {

            using id = std::size_t;


            template<typename T>
            class device final {
            public:
                using value_type = T;
                using const_referens = const T &;
                using referen = T &;
                using pointer = value_type *;
                using lock = std::lock_guard<std::mutex>;

                device() = default;

                device(const device &) = delete;

                device &operator=(const device &) = delete;

                ~device() = default;

                auto push(value_type &&value) -> void {
                    auto current_id = value->id();
                    lock _(mutex_);
                    storage.emplace(current_id, std::move(value));
                    queue_.emplace_back(current_id);

                }

                auto pop_all(std::vector<std::size_t> &contaner) -> std::size_t {

                    lock _(mutex_);

                    if (queue_.empty()) {
                        return 0;
                    }

                    contaner.reserve(queue_.size());
                    for (auto &&i:queue_) {
                        contaner.emplace_back(std::move(i));
                    }

                    queue_.clear();
                    cv.notify_all();
                    return contaner.size();

                }

                auto get(id current_id) -> referen {
                    lock _(mutex_);
                    return storage.at(current_id);

                }

                auto in(id current_id) -> bool {
                    lock _(mutex_);
                    auto status = false;
                    status = !(storage.find(current_id) == storage.end());
                    return status;

                }

                auto release(std::size_t id) -> void {
                    lock _(mutex_);
                    storage.erase(id);
                }

            private:
                std::mutex mutex_;
                std::condition_variable cv;
                std::deque<id> queue_;
                std::unordered_map<std::size_t, value_type> storage;
            };

}}}