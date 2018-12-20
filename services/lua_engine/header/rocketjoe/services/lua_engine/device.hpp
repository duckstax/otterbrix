#pragma once
#include <unordered_map>
#include <thread>
#include <forward_list>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include <rocketjoe/api/transport_base.hpp>

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
                    auto tmp = api::create_transport(value->type(),value->id());
                    lock _(mutex_);
                    storage.emplace(current_id,std::make_pair(std::move(value),std::move(tmp)));
                    queue_.emplace_back(current_id);
                    cv.notify_all();

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

                auto get_first(id current_id) -> referen {
                    lock _(mutex_);
                    return storage.at(current_id).first;

                }

                auto get_second(id current_id) -> referen {
                    lock _(mutex_);
                    return storage.at(current_id).second;

                }

                auto in(id current_id) -> bool {
                    lock _(mutex_);
                    auto status = false;
                    status = !(storage.find(current_id) == storage.end());
                    return status;

                }

                auto release(std::size_t id) {
                    lock _(mutex_);
                    auto it = storage.find(id);
                    if( it != storage.end() ){
                        value_type tmp = std::move(it->second.second);
                        storage.erase(it);
                        return tmp;
                    }
                }

            private:
                std::mutex mutex_;
                std::condition_variable cv;
                std::deque<id> queue_;
                std::unordered_map<std::size_t, std::pair<value_type,value_type>> storage;
            };

}}}