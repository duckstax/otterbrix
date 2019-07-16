#pragma once
#include <unordered_map>
#include <thread>
#include <forward_list>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include <sol.hpp>

namespace rocketjoe { namespace services { namespace lua_engine {

            template<typename QUERY, typename REQ,typename RES>
            class device final {
            public:
                using lock = std::lock_guard<std::mutex>;
                using id_t = std::size_t;

                device() = default;

                device(const device &) = delete;

                device &operator=(const device &) = delete;

                ~device() = default;

                auto push(QUERY &&value) -> void {
                    auto current_id = value.id();

                    {
                        lock _(mutex_);
                        storage.emplace(current_id, std::move(value));
                        queue_.emplace_back(current_id);
                    }
                        cv.notify_one();

                }

                auto pop_all(sol::table jobs) {
                    std::size_t size = 0;

                    {
                        lock _(mutex_);

                        if (queue_.empty()) {
                            return size;
                        }

                        for ( auto&& v : queue_ ) {
                            jobs[size] = std::move(v);
                            ++size;
                        }

                        queue_.clear();
                    }

                    cv.notify_one();

                    return size;

                }

                auto get_first(id_t current_id) ->REQ& {
                    lock _(mutex_);
                    return storage.at(current_id).request();

                }

                auto get_second(id_t current_id) ->RES& {
                    lock _(mutex_);
                    return storage.at(current_id).response();

                }

                auto in(id_t current_id) -> bool {
                    lock _(mutex_);
                    auto status = false;
                    status = !(storage.find(current_id) == storage.end());
                    return status;

                }

                auto release(id_t id) {
                    lock _(mutex_);
                    auto it = storage.find(id);
                    if( it != storage.end() ){
                        it->second.write();
                        storage.erase(it);
                    }
                }

            private:
                std::mutex mutex_;
                std::condition_variable cv;
                std::deque<id_t> queue_;
                std::unordered_map<std::size_t, QUERY> storage;
            };

}}}