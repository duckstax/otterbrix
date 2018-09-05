#pragma once
#include <unordered_map>
#include <thread>
#include <forward_list>
#include <mutex>
#include <condition_variable>
#include <deque>

namespace RocketJoe { namespace services { namespace lua_engine { namespace lua_vm {

                using id = std::size_t;


                template<typename T>
                class device final {
                public:
                    using clock = std::chrono::steady_clock;
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
                        auto current_id = static_cast<std::size_t>(std::chrono::duration_cast<std::chrono::microseconds>(clock::now().time_since_epoch()).count());
                        mutex_.lock();
                        storage.emplace(current_id, std::move(value));
                        queue_.emplace_back(current_id);
                        mutex_.unlock();

                    }

                    auto pop() -> id {
                        lock _(mutex_);
                        if (queue_.empty()) {
                            return 0;
                        }
                        id current_id = queue_.front();
                        queue_.pop_front();
                        return current_id;
                    }

                    template< typename C >
                    auto pop_all(C &contaner) -> void {
                        mutex_.lock();
                        for (auto &i:queue_) {
                            contaner.emplace_back(i);
                        }
                        mutex_.unlock();
                    }

                    auto get(std::size_t current_id) -> referen {
                        lock _(mutex_);
                        return storage.at(current_id);

                    }

                    auto in(std::size_t current_id) -> bool {
                        lock _(mutex_);
                        auto status = false;
                        auto it = storage.find(current_id);
                        if(it == storage.end()){
                            status=false;
                        } else {
                            status= true;
                        }

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
}}}}