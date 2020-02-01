#pragma once

#include <atomic>
#include <unordered_map>
#include <functional>
#include <chrono>

#include <boost/chrono/chrono_io.hpp>

#include <goblin-engineer.hpp>
#include <goblin-engineer/components/network.hpp>

namespace rocketjoe { namespace network {

    using timer_id = uint64_t;

    class timer final {
    private:
        using period_t = std::chrono::microseconds;
        using context = boost::asio::io_context;
        using timer_action = std::function<void()>;
        using timer_type = boost::asio::steady_timer;
    public:

        timer() = delete;
        timer(const timer&) = delete;


        template<class P, class F>
        timer(context &ctx, P &&interval, F &&f)
                : work_(boost::asio::make_work_guard(ctx))
                , timer_(ctx)
                , periodicity_(true)
                , period_(std::chrono::duration_cast<period_t>(interval))
                , timer_operation_(std::forward<F>(f))
                , enabled_(true) {
            start();
        }

        template<class F>
        timer(context &ctx, F &&f)
                : work_(boost::asio::make_work_guard(ctx))
                , timer_(ctx)
                , periodicity_(false)
                , timer_operation_(std::forward<F>(f))
                , enabled_(true) {
            start();
        }

        ~timer() {
            cancel();
        }


        void operator()(boost::system::error_code e) {
            if (e) {
                cancel();
                return;
            } else {
                if (enabled_) {
                    timer_operation_();
                }
                if (periodicity_) {
                    start();
                }
                return;
            }
        }


        void cancel() {
            enabled_ = false;
            timer_.cancel();
        }

    private:

        void start() {
            timer_.expires_from_now(period_);
            timer_.async_wait(std::ref(*this));
        }

        boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
        timer_type timer_;
        const bool periodicity_;
        period_t period_;
        timer_action timer_operation_;
        std::atomic_bool enabled_;
    };

    using timer_manager_storage = std::unordered_map<timer_id, timer>;

    class timer_manager final : public goblin_engineer::components::network_manager_service {
    public:
        timer_manager(
                goblin_engineer::root_manager *,
                goblin_engineer::dynamic_config &
        );


        template<class P, class F>
        timer_id add_timer(P &&period, F &&f) {
            auto current_counter = increment();
            timer_manager_storage_.emplace(current_counter, timer(loop(), period, std::forward<F>(f)));
            return current_counter;
        }

        template<class F>
        timer_id add_timer(F &&f) {
            auto current_counter = increment();
            timer_manager_storage_.emplace(current_counter, std::move(timer(loop(), std::forward<F>(f))));
            return current_counter;
        }

        void cancel(timer_id id);

        ~timer_manager() override = default;

    private:
        uint64_t increment() {
            return counter_timer_++;
        }

        std::atomic_uint64_t counter_timer_;
        timer_manager_storage timer_manager_storage_;

    };

}}
